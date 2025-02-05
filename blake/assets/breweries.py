import requests

from dagster import (
    AssetExecutionContext,
    asset,
    Output,
    asset_check,
    AssetCheckResult,
    AssetCheckExecutionContext,
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
from dagster._utils.backoff import backoff
import duckdb
from datetime import datetime
import os

BREWERIES_URL = os.getenv("BREWERIES_URL")
DAGSTER_PIPES_BUCKET = os.getenv("DAGSTER_PIPES_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
DUCKDB_DATABASE = os.getenv("DUCKDB_DATABASE")
MINIO_HOST = os.getenv("MINIO_HOST")
MINIO_PORT = os.getenv("MINIO_PORT")

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


@asset(tags={"domain": "data", "pii": "false"})
def breweries_api_health(context: AssetExecutionContext) -> Output:
    api_url = f"{BREWERIES_URL}/breweries"

    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        current_date = datetime.now().strftime("%Y%m%d")
        return Output(
            value="Breweries API healthly",
            metadata={"check_date": f"{current_date}"},
        )

    except Exception as e:
        context.log.error(f"API check failed: {str(e)}")
        raise e


@asset_check(asset=breweries_api_health, blocking=True)
def check_breweries_contract(context: AssetCheckExecutionContext):
    contract_keys = set(
        [
            "id",
            "name",
            "brewery_type",
            "address_1",
            "address_2",
            "address_3",
            "city",
            "state_province",
            "postal_code",
            "country",
            "longitude",
            "latitude",
            "phone",
            "website_url",
            "state",
            "street",
        ]
    )
    api_url = f"{BREWERIES_URL}/breweries"
    response = requests.get(api_url, timeout=10)
    data = response.json()
    api_contract_keys = data[0].keys()
    return AssetCheckResult(
        passed=bool(contract_keys.issubset(set(api_contract_keys))),
    )


@asset(required_resource_keys={"spark"}, tags={"domain": "data", "pii": "false"})
def breweries_metadata(context: AssetExecutionContext) -> Output:
    spark = context.resources.spark.spark_session
    try:
        response = requests.get(f"{BREWERIES_URL}/breweries/meta", timeout=10)
        response.raise_for_status()
        data = response.json()

        # Convert string values to integers
        converted_data = {
            "total": int(data["total"]),
            "page": int(data["page"]),
            "per_page": int(data["per_page"]),
        }
    except (requests.exceptions.RequestException, KeyError, ValueError) as e:
        context.log.error(f"Data fetch failed: {str(e)}")
        raise RuntimeError(f"API request failed: {str(e)}") from e

    # Create DataFrame with explicit type conversion
    data_df = spark.createDataFrame(
        [converted_data],  # Wrap in list to create a single-row DataFrame
        schema=StructType(
            [
                StructField("total", IntegerType(), True),
                StructField("page", IntegerType(), True),
                StructField("per_page", IntegerType(), True),
            ]
        ),
    )

    output_path = f"s3a://{DAGSTER_PIPES_BUCKET}/raw/breweries/metadata/"
    data_df.write.mode("overwrite").json(output_path)

    return Output(
        value="Data loaded successfully",
        metadata={
            "num_records": data_df.count(),
            "output_path": output_path,
            "columns": list(data_df.columns),
            "values": converted_data,  # Show converted values in metadata
        },
    )


def fetch_page_from_api(page: int, per_page: int):
    try:
        url = f"{BREWERIES_URL}/breweries"
        params = {"page": page, "per_page": per_page}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Raise exception for 4xx/5xx
        return response.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API request failed for page {page}: {str(e)}") from e


@asset(
    deps=["breweries_api_health", "breweries_metadata"],
    required_resource_keys={"spark"},
    tags={"domain": "data", "pii": "false"},
)
def breweries_api(context: AssetExecutionContext) -> Output:
    spark = context.resources.spark.spark_session

    # Read pagination metadata
    metadata_df = spark.read.json(
        f"s3a://{DAGSTER_PIPES_BUCKET}/raw/breweries/metadata/"
    )
    metadata_row = metadata_df.first()
    total_records = int(metadata_row["total"])
    per_page = int(metadata_row["per_page"])
    total_pages = (total_records + per_page - 1) // per_page  # Ceiling division

    all_data = []
    for page in range(1, total_pages + 1):
        context.log.info(f"Fetching page {page}/{total_pages}")
        try:
            page_data = fetch_page_from_api(page, per_page)
            all_data.extend(page_data)
        except RuntimeError as e:
            context.log.error(f"Failed to fetch page {page}: {str(e)}")
            raise

    # Create DataFrame with schema validation
    data_df = spark.createDataFrame(all_data, schema=json_schema)

    # Write with partitioning by state
    output_path = f"s3a://{DAGSTER_PIPES_BUCKET}/raw/breweries/api/"
    data_df.write.mode("overwrite").json(output_path)

    return Output(
        value="Data loaded successfully",
        metadata={
            "num_records": data_df.count(),
            "output_path": output_path,
            "columns": list(data_df.columns),
            "partition_by": "state",
            "total_pages": total_pages,
            "per_page": per_page,
        },
    )


@asset(
    deps=["breweries_api"],
    required_resource_keys={"spark"},
    tags={"domain": "data", "pii": "false"},
)
def breweries_partioned_by_location_parquet(context: AssetExecutionContext) -> Output:
    columns_to_normalize = ["city", "state", "country"]
    spark = context.resources.spark.spark_session
    raw_df = spark.read.json(f"s3a://{DAGSTER_PIPES_BUCKET}/raw/breweries/api/")
    unidecoded_df = raw_df.select(
        [
            F.when(
                F.col(c).isNotNull(),
                F.regexp_replace(
                    F.regexp_replace(
                        F.lower(F.trim(F.col(c))),
                        r"\p{M}",  # Remove diacritics using Unicode property
                        "",
                    ),
                    r"\s+",  # Replace whitespace sequences
                    "_",
                ),
            )
            .otherwise(F.col(c))
            .alias(c)
            if c in columns_to_normalize
            else F.col(c)
            for c in raw_df.columns
        ]
    )

    # Add transformation current_timestamp
    timestamp = F.current_timestamp()
    transformed_df = unidecoded_df.withColumn("transformed_at", timestamp)

    # Write as Parquet partitioned by specified columns
    transformed_df.write.mode("overwrite").partitionBy(
        "country", "state", "city"
    ).parquet(f"s3a://{DAGSTER_PIPES_BUCKET}/silver/breweries/")
    current_timestamp_value = transformed_df.select("current_timestamp").collect()[0][0]
    return Output(
        value="Breweries API partitioned successfully",
        metadata={
            "timestamp": str(current_timestamp_value),
        },
    )


@asset(
    deps=["breweries_partioned_by_location_parquet"],
    tags={"domain": "sales", "pii": "false"},
)
def breweries_by_type_location() -> None:
    query = f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='{MINIO_HOST}:{MINIO_PORT}';
        SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';
        SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        create or replace table breweries_by_country as ( 
        select country, count(distinct(brewery_type)) as unique_types
        from read_parquet(
            's3a://{DAGSTER_PIPES_BUCKET}/silver/breweries/**/*.parquet',
            hive_partitioning=1
        )
        group by country);
        copy breweries_by_country to 's3a://{DAGSTER_PIPES_BUCKET}/gold/breweries/by_location.json' (format json, overwrite_or_ignore true);
        """
    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": DUCKDB_DATABASE,
        },
        max_retries=10,
    )
    result = conn.execute(query).fetch_df()
