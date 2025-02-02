import dagster as dg
import requests
from .assets import breweries

breweries_assets = dg.load_assets_from_modules([breweries])
breweries_job = dg.define_asset_job(
    name="breweries_raw_job",
    selection=[
        breweries.breweries_api,
        breweries.breweries_partioned_by_location_parquet,
    ],
)


@dg.sensor(
    job=breweries_job,
    minimum_interval_seconds=86400,  # 24 hours
)
def daily_brewery_sensor(context: dg.SensorEvaluationContext):
    api_url = "https://api.openbrewerydb.org/breweries"

    try:
        # Simple health check - adjust validation as needed
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()

        # Optional: Add basic content check if needed
        if response.status_code == 200:  # Modify based on your API's response
            return dg.RunRequest(run_key=f"daily_brewery{response.status_code}")

    except Exception as e:
        context.log.error(f"API check failed: {str(e)}")
        return  # Don't trigger the job


defs = dg.Definitions(
    assets=breweries_assets, jobs=[breweries_job], sensors=[daily_brewery_sensor]
)
