## BLake

A data lake medallion architecture proposal. This project collects data from the [Open Brewery DB API](https://www.openbrewerydb.org/) to retrieve and process global brewery information.

### Context

The [Open Brewery DB API](https://www.openbrewerydb.org/) is an open-source project that organizes public brewery data into a centralized repository. The dataset includes macro-level geographic information (e.g., country, state, city) and granular details such as addresses, geolocation coordinates, and brewery types.

This data enables insights into brewery distribution across regions. For example, companies could leverage geolocation data to prioritize sales efforts in specific cities or states.

However, raw API data requires transformation to become actionable for business analysis. This project implements a pipeline to periodically fetch, transform, and enrich the data for analytical use cases.

### Blake project

The Blake Project provides information from the "Open Brewery DB API" across three distinct data layers, each representing a stage of transformation.

#### Raw (Bronze) Layer

This layer stores data in its most "natural" state, mirroring the API's original format without transformations. To achieve this, the project retains the data in JSON format, preserving the API’s exact response structure. All data across layers is stored in the storage service, ensuring controlled access by authorized services or analysts with proper permissions.

#### Silver Layer

Here, the data undergoes light transformations to enhance usability. Key steps include:

* Renaming columns for clarity.

* Partitioning data by macro-geolocation (country, state, city) to optimize downstream analysis.

* Adding timestamps for data generation tracking.

* Normalizing column values to eliminate special characters, whitespace inconsistencies, or decoding issues.

These steps ensure partition consistency and reduce the need for analysts to cross-reference raw data for naming discrepancies.

#### Gold Layer

This layer delivers curated, insight-ready data. Analysts can explore silver-layer data directly, leveraging pre-processed transformations to generate actionable insights without redundant work. For example, the project aggregates breweries by type per macro geographic level (country, state, city), enabling immediate analysis. Additionally, users can explore further insights via the integrated Jupyter service, which provides access to both silver and raw layers.

By structuring data into these layers, the Blake Project streamlines analysis while maintaining traceability from raw sources to refined outputs.

#### Blake design

!["Project Diagram"](/images/blake-arch.png)

The project architecture was designed to achieve the goals described above with consistency and reliability. The layers must be generated in a specific order and updated periodically. To accomplish this, the project leverages the Dagster orchestration platform.

#### Dagster Orchestration

[Dagster](https://dagster.io/) uses the concept of assets—objects that represent persistent data in storage. Each asset is a code-based definition of how its corresponding data is generated. This declarative approach simplifies orchestration, enabling data engineers to trace transformations and logic without unnecessary complexity.

#### Spark Processing

The Blake Project primarily relies on [Apache Spark](https://spark.apache.org/) for computation. Spark’s distributed processing capabilities allow the project to:

* Handle large datasets sourced from the API.

* Parallelize transformations across multiple workers (via PySpark).

* Scale resources dynamically by increasing worker nodes as needed.

#### Minio Storage

All data is persisted in the [Minio](https://min.io/) storage service, which mimics cloud storage semantics. This design ensures flexibility, enabling seamless migration to other cloud providers (e.g., AWS S3, Google Cloud Storage) if required.

#### Jupyter Analytics

Analytics can be performed directly within the integrated [Jupyter](https://jupyter.org/) service, which provides interactive access to data across all layers.

#### Metabase Analytics

The project also contains a [Metabase](https://www.metabase.com/) container, this is the service to Analytics teams materialize insights discovered in the Jupyter Notebooks, the service already contains the DuckDB plugin to maintain the tools used by analysts in the Jupyter service, making easy to create the visualizations necessary.

#### Docker Deployment

To ensure the project is as production-ready as possible, core services (Spark, Minio, Jupyter, etc.) are containerized using [Docker](https://www.docker.com/). This allows the entire architecture to be replicated effortlessly across any cloud provider or on-premise environment.

#### Blake data Flow

From a business perspective, updating the brewery information does not need to be real-time. This data is useful for developing medium- and long-term strategies, with daily follow-ups. Therefore, we initially use a daily update strategy, though it can be easily modified if necessary.

To ensure the consistency and health of data-dependent analyses, we need to confirm that the API source is functioning correctly and aligns with our previous analyses. To achieve this, we have created an asset called `breweries_api_health`. This asset checks the health of the API and ensures that the contract used previously remains the same. Additionally, it triggers the pipeline for our raw and silver layers. We also created an asset called `breweries_metadata`, which provides information about the number of items and pagination, enabling us to retrieve all information from the API.

The asset is scheduled to run daily at 7 AM. If all checks are successful, the pipeline for the raw and silver layers is triggered. If not, a sensor monitors the asset and sends an alert to an endpoint. In this project, we are using a dummy endpoint, but this could be replaced with any action, including a Slack message.

The `breweries_api` and `breweries_partitioned_by_location_parquet` assets represent the materialization of our raw and silver layers, respectively. In Dagster, the naming convention for assets is designed to help engineers easily understand where to make changes or fixes when necessary. These assets have a downstream dependency, meaning the `breweries_partitioned_by_location_parquet` asset will only be generated if the `breweries_api` asset has been materialized. This pipeline also includes a sensor to alert the team if it terminates with an error. The goal is to monitor the core data of our project.

Finally, we have the `breweries_by_type_location` asset, which materializes an aggregation of the silver layer data. As such, it is upstream of the `breweries_partitioned_by_location_parquet` asset, meaning its materialization begins only after the previous asset has been materialized. To ensure this, we have created a sensor to check the success of the raw-to-silver pipeline, which triggers the materialization of the golden layer.

#### Handling Errors

* In/Out

  In the current version, we check if the API contract contains the expected keys based on our previous analyses. This check ensures that the expected keys are still present but does not trigger an alert if new keys are added. A failure is emitted only if the API contract is missing keys. This approach was chosen to guarantee that the pipeline will not behave abnormally in the event of missing keys.

#### Evolutions

The project can evolve into a more robust design with the addition of several features.

* Add granular permissions to data storage. Ideally, the data storage should have different access permissions for different groups to guarantee data preservation. For example, the analytics teams should only have read access to the silver layer and should not access the raw layer.
* Improve ingestion using specialized tools such as [DLT](https://dlthub.com/blog/dlt-dagster). The idea is to add a tool focused on the ingestion process, delegating the responsibility of checking contracts and automatically handling changes.
* Improve model schema definitions and evolutions using [DBT](https://docs.dagster.io/integrations/libraries/dbt) to better handle schema changes, additions and evolutions of data models in the gold layer.
* Logging and monitoring using tools like [Metaplane](https://www.metaplane.dev/) or [ATLAN](https://atlan.com/) to simplify access to data quality insights, logging and monitoring downstream code changes impact.

#### RUN

In order to run the project, make sure that you have docker, docker compose and make installed.

The project contains a Makefile with two commands `up` and `down`, after cloning the repository, in the project directory run:

```
make up
```

With that, all containers will run. In order to trigger all the pipelines, go to:
<http://localhost:3000/> > Automation > check all automations > Actions > start.

Done, now the Dagster orchestrator will start the pipelines with their respective checks and sensors.

The project contains two ways to make analytic analyses.
In <http://localhost:8888>, you w:will find a jupyter notebook application, with a example notebook with how it is possible to access the storage using DuckDB.

It is also possible to use DuckDB in the Metabase application (<http://localhost:3030>) and using the DuckDB plugin installed, it is possible to run queries and create visualizations in the same way as the notebook example.
