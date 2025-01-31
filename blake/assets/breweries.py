from typing import Dict, Any
import requests

from dagster import asset, Output
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

json_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True),
    ]
)


@asset
def HelloWorld() -> None:
    print("Hello World")


def fetch_data_from_api():
    try:
        response = requests.get("https://api.openbrewerydb.org/breweries", timeout=10)
        response.raise_for_status()  # Raise exception for 4xx/5xx
        return response.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API request failed: {str(e)}") from e


@asset
def breweries_api() -> Output:
    spark = (
        SparkSession.builder.appName("ExtractBreweries")
        .master("spark://spark:7077")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    # Fetch data with error handling
    try:
        data = fetch_data_from_api()
    except RuntimeError as e:
        context.log.error(f"Data fetch failed: {str(e)}")
        raise

    # Create DataFrame with schema validation
    data_df = spark.createDataFrame(data, schema=json_schema)

    # Write with partitioning by state
    output_path = "s3a://blake/raw-layer/breweries/"
    data_df.write.mode("overwrite").json(output_path)

    return Output(
        value="Data loaded successfully",
        metadata={
            "num_records": data_df.count(),
            "output_path": output_path,
            "columns": list(data_df.columns),
            "partition_by": "state",
        },
    )
