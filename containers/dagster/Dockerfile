ARG SPARK_VERSION=3.5.1

FROM bitnami/spark:${SPARK_VERSION}

USER root

ARG SPARK_VERSION=3.4.1

COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /bin/

RUN install_packages curl

RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$SPARK_VERSION/hadoop-aws-$SPARK_VERSION.jar" \
  -o /opt/bitnami/spark/jars/hadoop-aws-$SPARK_VERSION.jar

RUN curl -fSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.780/aws-java-sdk-bundle-1.12.780.jar" \
  -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.780.jar

# install Python dependencies
RUN --mount=type=cache,target=/root/.cache/uv uv pip install --system dagster dagster-postgres dagster-docker dagster-duckdb pyspark numpy pandas
USER root
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  default-jdk

WORKDIR /opt/dagster/app
COPY . /opt/dagster/app/

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "--port", "4000", "-m", "blake"]

