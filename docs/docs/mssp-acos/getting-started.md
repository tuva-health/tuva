---
id: getting-started
title: "Getting Started"
hide_title: true
---

# Getting Started with the MSSP ACO Pipeline

This guide walks through setting up and running the full pipeline, from downloading raw CMS files to running Tuva analytics and calculating projected savings.

## Prerequisites

Before you begin, you will need:

- **CMS Datahub access** — Credentials for the ACO Management System (ACOMS) to download your ACO's data files
- **A supported data warehouse** — One of: Snowflake, Databricks, BigQuery, Redshift, MotherDuck, or a local DuckDB/Parquet setup
- **Python 3.10+** and [uv](https://github.com/astral-sh/uv) for running the MSSP Pipeline
- **dbt** installed and configured for your data warehouse
- **Your ACO ID** — The CMS-assigned identifier for your ACO

## Step 1: Go to the CMS Portal and setup API Credentials

Go to the [ACO Management System Portal](https://acoms.cms.gov/login) and navigate to the `API Credentials` tab. Once there, under `ACO-MS API Credentials` click `Create New Credentials`, you will need to give the API credential a name (i.e. `MSSP Data Pipeline`), then select `Credential delegate (API)` for the API Key access level and `Data Hub` as the Resource. You will also need to input the IP address of the machine that will be running the MSSP pipeline.

Store the API credentials is a safe location to be used in a later step.

## Step 2: Download and Load CMS Data

After getting your we will need to clone the MSSP Pipeline and configure it's dependencies and credentials. The MSSP pipeline does the heavy lifting by downloading, unpacking the ACO's files from the CMS Datahub and then loads them into your data warehouse.

### Clone and configure the MSSP Pipeline

```bash
git clone <your-repo-url> mssp_pipeline
cd mssp_pipeline
```

### Install dependencies
Example of installing dependencies, see [MSSP Pipeline](mssp-pipeline) for more options depending on your cloud provider and warehouse.
```bash
# Install with your desired output backend, e.g. Snowflake:
uv sync --extra processing --extra snowflake
```

### Configure API credentials (one-time)
This launches the ACOMS CLI and asks you to save your the `ACO-MS API Credentials` we acquired earlier.
```bash
uv run mssp-download --configure
```


### Edit `.env` with connection info

Next we need to configure the `MSSP Pipeline` with information on what ACO ID we will be transmitting data for, and what `FILE_STORE` and `OUTPUT_TYPE` to use in the integrations. 

```python
ACO_ID = "A1234"          # Your CMS ACO identifier
FILE_STORE = "s3://my/mssp/data/location"  # Local directory to store downloaded files

OUTPUT_TYPE = "SNOWFLAKE"  # Your data warehouse backend
```

Next, add configuration for your warehouse connection settings in the same file (e.g., `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_DATABASE`, etc.). See the [MSSP Pipeline](mssp-pipeline) page for a full list of configurations for the supported data warehouses.

### Run the pipeline

```bash
# Download files from CMS Datahub Only
uv run mssp-download

# Take already downloaded files and transform and load to your warehouse
uv run mssp-process

# Or run both in sequence
uv run mssp-pipeline
```

After this step, your warehouse will contain raw CCLF tables, ALR tables, and other MSSP data files.

## Step 3: Run the CMS MSSP Connector

The [CMS MSSP Connector](cms-mssp-connector) runs immediately after the pipeline to build source objects and intermediate models for all MSSP report files that are not AALR or CCLF (benchmark expenditures, quality measures, shadow bundles, non-claims payments, and more).

### Clone and configure the CMS MSSP Connector
```bash
git clone <your-repo-url> cms_mssp_connector
cd mssp_pipeline
```

### Install dependencies

```bash
uv sync
```

### Configure

In `cms_mssp_connector/dbt_project.yml`, point to the raw MSSP source data:

```yaml
vars:
  input_database: "your_database"
  input_schema: "your_mssp_schema"
```

### Run the CMS MSSP Connector

```bash
dbt deps
dbt build
```

This creates staging and intermediate models for all MSSP report files. In addition, the MSSP connector will run the ALR and CCLF connectors to populate the data warehouse with all core and enriched datamarts provided by Tuva.

## Step 3: Run the Tuva Project

With the Tuva Input Layer populated, run the Tuva Project to generate the Core Data Model and all data marts. This can be run seamlessly from the CMS MSSP connector, but can also be triggered separately if desired.

### Configure

In your Tuva project's `dbt_project.yml`, point to the output schema from Step 4:

```yaml
vars:
  input_database: "your_database"
  input_schema: "your_cclf_output_schema"
```

### Run

```bash
cd tuva
dbt deps
dbt build
```


## Step 4: Deploy ACO dashboards

With the Tuva data models available in your data warehouse. We can now deploy the ACO dashboards, note that these are only available in Power BI.


## Pipeline Summary

| Step | Tool | Input | Output |
|---|---|---|---|
| 1 | mssp_pipeline | CMS Datahub | All raw MSSP tables in warehouse |
| 2 | cms_mssp_connector (phase 1) | Raw MSSP tables | Staging + intermediate MSSP models |
| 3 | cms_alr_connector | AALR tables | `enrollment` table |
| 4 | medicare_cclf_connector | CCLF tables + enrollment | `eligibility`, `medical_claim`, `pharmacy_claim` |
| 5 | Tuva Project | Tuva Input Layer | Core Data Model + data marts |
| 6 | MSSP ACO Power BI Dashboards | Core Data Model + data marts  | Power BI dashboards |
