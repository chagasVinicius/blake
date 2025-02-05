import dagster as dg
import requests
from ..jobs import breweries_api_health_job, breweries_main_job


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
def run_breweries_golden_sensor():
    return dg.RunRequest()


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
