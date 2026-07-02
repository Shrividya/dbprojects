# dbprojects

A collection of Databricks projects, managed as [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html).

## Projects

### [retail_lakehouse](retail_lakehouse/)

A medallion-architecture lakehouse for NYC taxi trip data (bronze → silver → gold) built with PySpark and Databricks Delta tables.

- **Bronze** — raw ingestion of taxi trips (streaming, Auto Loader) and taxi zone lookups (batch CSV) into `retail_lakehouse.bronze.*`
- **Silver** — deduplicates trips and enriches them with pickup/dropoff borough and zone info into `retail_lakehouse.silver.*`
- **Gold** — aggregates enriched trips into a daily revenue table (`retail_lakehouse.gold.daily_revenue_table`) by date and pickup borough

Each stage is deployed as a Databricks job defined in `retail_lakehouse/resources/`. See [retail_lakehouse/README.md](retail_lakehouse/README.md) for setup and deployment instructions using the Databricks CLI.

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) for Python dependency management
- Access to a Databricks workspace
