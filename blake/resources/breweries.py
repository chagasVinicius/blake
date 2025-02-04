from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr
from pyspark.sql import SparkSession
import os

SPARK_URL = os.getenv("SPARK_URL", "")


class PySparkResource(ConfigurableResource):
    """Simplified PySpark resource with fixed configuration"""

    app_name: str = "ExtractBreweries"
    master_url: str = SPARK_URL
    # master_url: str = "spark://spark:7077"
    _spark_session: SparkSession = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        minio_endpoint = os.getenv("AWS_ENDPOINT_URL")
        self._spark_session = (
            SparkSession.builder.appName(self.app_name)
            .master(self.master_url)
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            # .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .getOrCreate()
        )

    @property
    def spark_session(self) -> SparkSession:
        return self._spark_session
