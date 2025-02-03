## BLake

A data lake medallion architecture proposal. This project collects data from the [Open Brewery DB API](https://www.openbrewerydb.org/) to retrieve and process global brewery information.

### Context

The [Open Brewery DB API](https://www.openbrewerydb.org/) is an open-source project that organizes public brewery data into a centralized repository. The dataset includes macro-level geographic information (e.g., country, state, city) and granular details such as addresses, geolocation coordinates, and brewery types.

This data enables insights into brewery distribution across regions. For example, companies could leverage geolocation data to prioritize sales efforts in specific cities or states.

However, raw API data requires transformation to become actionable for business analysis. This project implements a pipeline to periodically fetch, transform, and enrich the data for analytical use cases.

### Blake project

!["Project Diagram"](/images/blake-arch.png)
The Blake Project provides information from the "Open Brewery DB API" across three distinct data layers, each representing a stage of transformation.

#### Raw (Bronze) Layer

This layer stores data in its most "natural" state, mirroring the API's original format without transformations. To achieve this, the project retains the data in JSON format, preserving the API’s exact response structure. All data across layers is stored in the Minio storage service, ensuring controlled access by authorized services or analysts with proper permissions.

#### Silver Layer

Here, the data undergoes light transformations to enhance usability. Key steps include:

* Renaming columns for clarity.

* Partitioning data by macro-geolocation (country, state, city) to optimize downstream analysis.

* Adding timestamps for data generation tracking.

* Normalizing column values to eliminate special characters, whitespace inconsistencies, or decoding issues.

These steps ensure partition consistency and reduce the need for analysts to cross-reference raw data for naming discrepancies.

#### Gold Layer

This layer delivers curated, insight-ready data. Analysts can explore silver-layer data directly, leveraging pre-processed transformations to generate actionable insights without redundant work. For example, the project aggregates breweries by type per city, enabling immediate analysis. Additionally, users can explore further insights via the integrated Jupyter service, which provides access to both silver and raw layers.

By structuring data into these layers, the Blake Project streamlines analysis while maintaining traceability from raw sources to refined outputs.

#### Blake design

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

#### Docker Deployment

To ensure the project is as production-ready as possible, core services (Spark, Minio, Jupyter, etc.) are containerized using [Docker](https://www.docker.com/). This allows the entire architecture to be replicated effortlessly across any cloud provider or on-premise environment.

#### Blake pipelines [WIP]

* Raw-Silver (schedule every day at 7AM)
  * Check API health
  * Check API metadata - an specific endpoint to check the metadata that exists in breweries API
  * IF Checks OK
    * Collect API data
    * IF SUCCESS
      * Generate silver
* Golden
  * Sensor to check asset silver layer
  * IF asset run SUCCESS
    * Generate aggregated data information

#### Blake Monitoring WIP

* Sensor RAW-SILVER pipeline
  * IF FAILURE
  * Call slack client to alert

#### RUN TODO

#### CONFIGURATION TODO

#### ANALYTICS TODO
