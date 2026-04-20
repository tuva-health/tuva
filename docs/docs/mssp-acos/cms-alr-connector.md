---
id: cms-alr-connector
title: "CMS ALR Connector"
hide_title: true
---

# CMS ALR Connector

The CMS ALR Connector is a dbt project that transforms CMS Advanced ACO Assignment List Reports (ALR) into the enrollment input expected by the [Medicare CCLF Connector](medicare-cclf-connector). It is designed to run after the [MSSP Pipeline](mssp-pipeline) has loaded the raw ALR files into your warehouse.

## What Are ALR Reports?

CMS provides monthly Assignment List Reports to MSSP ACOs identifying which Medicare beneficiaries are assigned to the ACO for a given performance year. The connector expects the raw ALR data to be organized using the latest CMS ALR format and source tables defined in the repo's `_sources.yml`. The Advanced ACO variant includes:

- **ALR1** — Assigned beneficiaries and enrollment flags (12 monthly flags per beneficiary)
- **ALR2** — Assigned beneficiaries by TIN
- **ALR4** — Assigned beneficiaries by TIN and NPI
- **ALR5** — Beneficiary turnover (newly assigned and dropped)
- **ALR6** — Beneficiaries eligible for voluntary alignment
- **ALR9** — Beneficiaries flagged as underserved

Each ALR file name is used to derive metadata such as performance year and reporting period. The connector relies on the `file_name` field to determine file precedence when multiple iterations exist for the same period.

## Model Layers

The connector follows a standard three-layer dbt architecture:

### Staging (views)

Type-casting only — no business logic. One staging model per ALR table:

| Model | Source |
|---|---|
| `stg_ALR1_assigned_beneficiaries` | ALR1 |
| `stg_ALR2_assigned_beneficiaries_tin` | ALR2 |
| `stg_ALR4_assigned_beneficiaries_tin_npi` | ALR4 |
| `stg_ALR5_beneficiary_turnover` | ALR5 |
| `stg_ALR6_beneficiaries_assignable_or_voluntary` | ALR6 |
| `stg_ALR9_beneficiaries_underserved` | ALR9 |

### Intermediate (tables)

Two models that join and reshape the staging data:

**`ALR_history`** — Joins all six ALR staging models and pivots the 12 monthly enrollment flag columns into individual rows, producing one row per beneficiary per enrollment month. It enriches each row with turnover, voluntary alignment, and underserved flags, then deduplicates by TIN and NPI using `dbt_utils.deduplicate()`.

**`ALR_history_filtered`** — Filters `ALR_history` to the highest-priority ALR file per enrollment month and performance year. Priority is determined by the `mssp_file_parameters` seed.

### Final (tables)

**`enrollment`** — The output consumed by the Medicare CCLF Connector. Contains one row per beneficiary per enrollment month with `enroll_flag > 0`. Key columns:

| Column | Description |
|---|---|
| `member_month` | Enrollment month in YYYYMM format |
| `enrollment_start_date` | First day of the enrollment month |
| `enrollment_end_date` | Last day of the enrollment month |
| `current_bene_mbi_id` | Current Medicare Beneficiary Identifier |
| `bene_hic_num` | Health Insurance Claim number |

**`provider_attribution`** — Provider attribution output by TIN and NPI in Tuva's payer attribution format.

## File Priority Seed

The `seeds/mssp_file_parameters.csv` seed maps ALR file iterations to performance periods and assigns a `priority` value. When multiple iterations of an ALR file exist for the same performance year and month, the connector uses the highest-priority file and discards the others.

The current seed covers performance years 2016 through 2026.

## Configuration

Set these variables in `dbt_project.yml` or via `--vars` on the command line:

```yaml
vars:
  input_database: "your_database"   # Database containing raw ALR tables
  input_schema: "your_mssp_schema"  # Schema containing raw ALR tables
  provider_attribution_enabled: true
  cms_alr_connector: true
  # Optional schema prefix for multi-tenant deployments
  tuva_schema_prefix: "optional_prefix"
```

## How to Run

```bash
cd cms_alr_connector

# Install dbt dependencies
dbt deps

# Build all models and run tests
dbt build

# Run a specific model
dbt run --select enrollment
```

## Output

The `enrollment` table is written to the target schema configured in your dbt profile. Point the [Medicare CCLF Connector](medicare-cclf-connector) at this table to provide member enrollment data for eligibility generation.

By default, the repo writes intermediate staging/history models to schema names derived from `input_layer`, while the final `enrollment` model is configured for `raw_data`. If `tuva_schema_prefix` is supplied, those schemas are prefixed automatically.

## Supported Databases

| Database | Supported |
|---|---|
| BigQuery | Yes |
| Databricks | Yes |
| Fabric | Yes |
| MotherDuck | Yes |
| Redshift | Yes |
| Snowflake | Yes |

Cross-database compatibility is implemented via dbt adapter dispatch macros (`cast_numeric`, `try_to_cast_date`, `extract_file_metadata`).

## Project Structure

```
cms_alr_connector/
├── dbt_project.yml
├── models/
│   ├── staging/           # One view per ALR source table
│   ├── intermediate/      # ALR_history, ALR_history_filtered
│   └── final/             # enrollment, provider_attribution
├── macros/                # extract_file_metadata, cast_numeric, try_to_cast_date
├── seeds/
│   └── mssp_file_parameters.csv  # File metadata and priority mapping (2016-2026)
└── tests/                 # dbt data tests
```
