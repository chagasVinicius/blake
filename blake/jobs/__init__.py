import dagster as dg
from ..assets import breweries

breweries_api_health_job = dg.define_asset_job(
    name="breweries_api_health_job",
    selection=[breweries.breweries_api_health],
)

breweries_main_job = dg.define_asset_job(
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
