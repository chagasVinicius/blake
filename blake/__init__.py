import dagster as dg
import requests
from .assets import breweries
from .resources.breweries import PySparkResource

breweries_assets = dg.load_assets_from_modules([breweries])

breweries_api_health_job = dg.define_asset_job(
    name="breweries_api_health_job",
    selection=[breweries.breweries_api_health],
)

breweries_api_health_schedule = dg.ScheduleDefinition(
    job=breweries_api_health_job, cron_schedule="*/4 * * * *"
)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.FAILURE,
    monitored_jobs=[breweries_api_health_job],
)
def check_breweries_health(context: dg.SensorEvaluationContext):
    ### Endpoint to alert
    response = requests.get("https://www.google.com")
    ###
    context.log.info("FAILURE ACTION")
    context.log.warning(f"{response.status_code}")


breweries_main_job = dg.define_asset_job(
    name="breweries_raw_silver",
    selection=[
        breweries.breweries_metadata,
        breweries.breweries_api,
        breweries.breweries_partioned_by_location_parquet,
    ],
)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[breweries_api_health_job],
    request_job=breweries_main_job,
)
def run_breweries_main_sensor(context: dg.SensorEvaluationContext):
    return dg.RunRequest()


@dg.asset_sensor(
    asset_key=dg.AssetKey("breweries_partioned_by_location_parquet"),
    job_name="breweries_golden_job",
)
def check_breweries_silver():
    return dg.RunRequest()


breweries_golden_job = dg.define_asset_job(
    name="breweries_golden_job", selection=[breweries.breweries_by_type_location]
)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.FAILURE,
    monitored_jobs=[breweries_main_job],
)
def check_breweries_main_job(context: dg.SensorEvaluationContext):
    ### Endpoint to alert
    response = requests.get("https://www.google.com")
    ###
    context.log.info("FAILURE ACTION")
    context.log.warning(f"{response.status_code}")


defs = dg.Definitions(
    assets=breweries_assets,
    jobs=[breweries_main_job, breweries_golden_job, breweries_api_health_job],
    asset_checks=[breweries.check_breweries_contract],
    sensors=[
        check_breweries_main_job,
        check_breweries_health,
        run_breweries_main_sensor,
        check_breweries_silver,
    ],
    resources={"spark": PySparkResource()},
    schedules=[breweries_api_health_schedule],
)
