import dagster as dg
from .assets import breweries
from .jobs import breweries_api_health_job, breweries_main_job, breweries_golden_job
from .sensors import (
    check_breweries_main_job,
    check_breweries_health,
    run_breweries_golden_sensor,
    run_breweries_main_sensor,
)
from .resources.breweries import PySparkResource

breweries_assets = dg.load_assets_from_modules([breweries])

breweries_api_health_schedule = dg.ScheduleDefinition(
    job=breweries_api_health_job,
    # To replicate the behaviour to run locally
    cron_schedule="*/5 * * * *",
    # To run daily at 7AM
    # cron_schedule = "0 7 * * *"
)


defs = dg.Definitions(
    assets=breweries_assets,
    jobs=[breweries_main_job, breweries_golden_job, breweries_api_health_job],
    asset_checks=[breweries.check_breweries_contract],
    sensors=[
        check_breweries_main_job,
        check_breweries_health,
        run_breweries_main_sensor,
        run_breweries_golden_sensor,
    ],
    resources={"spark": PySparkResource()},
    schedules=[breweries_api_health_schedule],
)
