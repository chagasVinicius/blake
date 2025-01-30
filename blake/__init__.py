from dagster import Definitions, load_assets_from_modules

from .assets import breweries

breweries_assets = load_assets_from_modules([breweries])

defs = Definitions(assets=breweries_assets)
