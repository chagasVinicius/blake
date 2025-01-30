import requests

from dagster import asset


@asset
def HelloWorld() -> None:
    print("Hello World")
