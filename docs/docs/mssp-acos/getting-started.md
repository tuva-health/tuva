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
- **Python 3.11+** and [uv](https://github.com/astral-sh/uv) for running the MSSP Pipeline
- **dbt** installed and configured for your data warehouse
- **Your ACO ID** — The CMS-assigned identifier for your ACO

## Step 1: Download and Load CMS Data

The [MSSP Pipeline](mssp-pipeline) downloads your ACO's files from the CMS Datahub and loads them into your data warehouse.

### Install

```bash
cd mssp_pipeline

# Install with your desired output backend, e.g. Snowflake:
uv sync --extra processing --extra snowflake
```

### Configure credentials (one-time)

```bash
uv run mssp-download --configure
```

This launches the ACOMS CLI to save your CMS Datahub credentials.

### Edit `config.py`

Open `mssp_pipeline/config.py` and set at minimum:

```python
ACO_ID = "A1234"          # Your CMS ACO identifier
FILE_STORE = "/data/mssp"  # Local directory to store downloaded files

OUTPUT_TYPE = "SNOWFLAKE"  # Your data warehouse backend
```

Configure your warehouse connection settings in the same file (e.g., `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_DATABASE`, etc.).

### Run the pipeline

```bash
# Download files from CMS Datahub
uv run mssp-download

# Transform and load to your warehouse
uv run mssp-process

# Or run both in sequence:
uv run mssp-pipeline
```

After this step, your warehouse will contain raw CCLF tables, ALR tables, and other MSSP data files.

## Step 2: Run the CMS MSSP Connector

The [CMS MSSP Connector](cms-mssp-connector) runs immediately after the pipeline to build source objects and intermediate models for all MSSP report files that are not AALR or CCLF (benchmark expenditures, quality measures, shadow bundles, non-claims payments, and more).

### Configure

In `cms_mssp_connector/dbt_project.yml`, point to the raw MSSP source data:

```yaml
vars:
  input_database: "your_database"
  input_schema: "your_mssp_schema"

  alr_database: "your_database"
  alr_schema: "your_alr_output_schema"

  cclf_database: "your_database"
  cclf_schema: "your_cclf_output_schema"

  tuva_database: "your_database"
  tuva_schema: "your_tuva_output_schema"
```

### Run

```bash
cd cms_mssp_connector
dbt deps
dbt build --exclude tag:enriched
```

This creates staging and intermediate models for all MSSP report files. In addition, the MSSP connector will run the ALR and CCLF connectors to populate the data warehouse with the full data being provided by CMS.

This produces `eligibility`, `medical_claim`, and `pharmacy_claim` tables in your warehouse.

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

This runs normalization, claims preprocessing, and all data marts — quality measures, financial PMPM, chronic conditions, readmissions, and more.


## Pipeline Summary

| Step | Tool | Input | Output |
|---|---|---|---|
| 1 | mssp_pipeline | CMS Datahub | All raw MSSP tables in warehouse |
| 2 | cms_mssp_connector (phase 1) | Raw MSSP tables | Staging + intermediate MSSP models |
| 3 | cms_alr_connector | AALR tables | `enrollment` table |
| 4 | medicare_cclf_connector | CCLF tables + enrollment | `eligibility`, `medical_claim`, `pharmacy_claim` |
| 5 | Tuva Project | Tuva Input Layer | Core Data Model + data marts |
| 6 | MSSP ACO Power BI Dashboards | Core Data Model + data marts  | Power BI dashboards |
