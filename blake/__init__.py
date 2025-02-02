import dagster as dg
from dagster._core.definitions import RunRequest
import requests
from .assets import breweries
from .resources.breweries import PySparkResource
from datetime import datetime

breweries_assets = dg.load_assets_from_modules([breweries])

breweries_job = dg.define_asset_job(
    name="breweries_raw_silver",
    selection=[
        breweries.breweries_metadata,
        breweries.breweries_api,
        breweries.breweries_partioned_by_location_parquet,
    ],
)

breweries_golden_job = dg.define_asset_job(
    name="breweries_golden_job", selection=[breweries.breweries_by_type_location]
)

breweries_api_health_job = dg.define_asset_job(
    name="breweries_api_health_job",
    selection=[breweries.breweries_api_health],
)

breweries_api_health_schedule = dg.ScheduleDefinition(
    job=breweries_api_health_job, cron_schedule="*/4 * * * *"
)


@dg.asset_sensor(
    asset_key=dg.AssetKey("breweries_partioned_by_location_parquet"),
    job_name="breweries_golden_job",
)
def check_breweries_job():
    return dg.RunRequest()


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[breweries_api_health_job],
    request_job=breweries_job,
)
def run_breweries_sensor(context: dg.SensorEvaluationContext):
    return dg.RunRequest()


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.FAILURE,
    monitored_jobs=[breweries_api_health_job],
)
def check_breweries_health(context: dg.SensorEvaluationContext):
    response = requests.get("https://api.openbrewerydb.org/v1/breweries/random")
    context.log.info("FAILURE ACTION")
    context.log.warning(f"{response.json()}")


defs = dg.Definitions(
    assets=breweries_assets,
    jobs=[breweries_job, breweries_golden_job, breweries_api_health_job],
    sensors=[check_breweries_job, check_breweries_health, run_breweries_sensor],
    resources={"spark": PySparkResource()},
    schedules=[breweries_api_health_schedule],
)
