from typing import Dict, Any
import requests

from dagster import asset, Output
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from dagster._utils.backoff import backoff
import duckdb

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
    output_path = "s3a://blake/raw/breweries/api/"
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


columns_to_normalize = ["city", "state_province", "country"]


@asset(deps=["breweries_api"])
def breweries_partioned_parquet() -> Output:
    spark = (
        SparkSession.builder.appName("BreweriesPartionedParquet")
        .master("spark://spark:7077")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    # Read raw JSON data from S3
    raw_df = spark.read.json("s3a://blake/raw/breweries/api/")
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
        "country", "state_province", "city"
    ).parquet("s3a://blake/silver/breweries/")
    current_timestamp_value = transformed_df.select("current_timestamp").collect()[0][0]
    return Output(
        value="Breweries API partitioned successfully",
        metadata={
            "timestamp": current_timestamp_value,
            "location": "blake/silver/breweries",
        },
    )


@asset(deps=["breweries_partioned_parquet"])
def breweries_table() -> Output:
    """
    The raw breweries API result, loaded into a DuckDB database
    """
    query = """
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    create or replace table breweries as (
       select
           id as id,
           name as name,
           brewery_type as type,
           city as city,
           state_province as state_province,
           country as country,
           latitude as latitude,
           website_url as url,
           state as state
        from read_parquet(
            's3a://blake/silver/breweries/**/*.parquet',
            hive_partitioning=1
        )
   );
   """
    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": "data/staging/data.duckdb",
        },
        max_retries=10,
    )
    with conn:
        # Execute table creation
        conn.execute(query)

        # Verify table existence and get metrics
        result = conn.sql("SELECT COUNT(*) AS row_count FROM breweries").fetchall()
        table_info = conn.sql("PRAGMA table_info('breweries')").fetchall()

    return Output(
        value=result[0][0],  # Return row count as primary value
        metadata={
            "database": "data/staging/data.duckdb",
            "table_name": "breweries",
            "row_count": result[0][0],
            "columns": [col[1] for col in table_info],  # Extract column names
            "s3_source": "s3a://blake/silver/breweries/**/*.parquet",
            "hive_partitioning": True,
        },
    )


@asset(deps=["breweries_table"])
def breweries_by_type_location() -> None:
    query = """
        select count(distinct(type))
        from breweries
        group by country
        """
    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": "data/staging/data.duckdb",
        },
        max_retries=10,
    )
    result = conn.execute(query).fetch_df()
    print("RESULT")
    print(result)
