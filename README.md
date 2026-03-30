[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.9.x&color=orange)

![diagram](./the-tuva-project-3.jpg)

## What is the Tuva Project?
The Tuva Project is a collection of tools for transforming raw healthcare data into analytics-ready data.  The main Tuva package (i.e. this repo) is a dbt package that includes the following components:
- Input Layer
- Claims Preprocessing
- Core Data Model
- Data Marts
- Terminology & Value Sets

Detailed documentation of this package and related tools, including data dictionaries, can be found at www.thetuvaproject.com.

## Agentic Workflow

We are increasingly using agents to use and further develop this package. You can find context for agents in [AGENTS.md](AGENTS.md).

## Contributing

This is the recommended setup for development:
- Python 3.10 or later
- duckdb
- dbt (dbt-core and dbt-duckdb)

Connect duckdb and dbt via your profile.yml.

Use tuva/integration_tests as your development project.  Configure the dbt_project.yml in this folder to connect to duckdb.

Run the package from integration_tests.  This will:
- Load package seed payloads from versioned S3 artifacts
- Build the entire pipeline in your duckdb instance

From there we recommend iterating with your preferred coding agent using [AGENTS.md](AGENTS.md).

Hello and welcome! Thank you so much for taking the time to contribute to the Tuva Project. People like you are helping to build a community of healthcare data practitioners that shares knowledge and tools. Whether it’s fixing a bug, submitting an idea, updating the docs, or sharing your healthcare knowledge, you can make an impact!

In this guide, you will get an overview of the contribution workflow, from how to contribute, setting up your development environment, testing, and creating a pull request.

## dbt Variables

Tuva uses dbt variables (`var()`) to control which parts of the pipeline are enabled and to configure runtime behavior. Set these in your `dbt_project.yml` under the `vars:` key.

### Data Source Enablement

These variables control which input data types are active. Setting a group-level variable enables all marts in that group; individual mart variables override the group setting.

| Variable | Default | Description |
|----------|---------|-------------|
| `claims_enabled` | `false` | Enable all claims-based marts |
| `clinical_enabled` | `false` | Enable all clinical-based marts |
| `tuva_marts_enabled` | `false` | Enable all marts (claims + clinical). Overridden by the more specific variables above. |

### Individual Mart Enablement

Each mart can be independently enabled or disabled. When not set, these inherit from `claims_enabled`, `clinical_enabled`, or `tuva_marts_enabled`.

| Variable | Default | Description |
|----------|---------|-------------|
| `claims_preprocessing_enabled` | Inherits | Claims preprocessing (encounters, service categories, normalized input) |
| `ccsr_enabled` | Inherits | Clinical Classifications Software Refined (CCSR) |
| `cms_chronic_conditions_enabled` | Inherits | CMS Chronic Conditions |
| `tuva_chronic_conditions_enabled` | Inherits | Tuva Chronic Conditions |
| `cms_hcc_enabled` | Inherits | CMS-HCC Risk Adjustment |
| `ed_classification_enabled` | Inherits | ED Classification |
| `financial_pmpm_enabled` | Inherits | Financial PMPM |
| `hcc_suspecting_enabled` | `false` | HCC Suspecting |
| `hcc_recapture_enabled` | `false` | HCC Recapture |
| `quality_measures_enabled` | Inherits | Quality Measures (HEDIS/CQM) |
| `readmissions_enabled` | Inherits | Readmissions |
| `fhir_preprocessing_enabled` | `false` | FHIR Preprocessing |
| `pqi_enabled` | Inherits | AHRQ Prevention Quality Indicators |
| `provider_attribution_enabled` | Inherits | Provider Attribution |
| `semantic_layer_enabled` | Inherits | Semantic Layer |
| `brand_generic_enabled` | Inherits | Brand/Generic pharmacy analysis |

### Runtime Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `tuva_last_run` | Current UTC timestamp | Populates the `tuva_last_run` column in all output models. Automatically set to the dbt run start time. |
| `cms_hcc_payment_year` | Current year | The CMS-HCC payment year for risk score calculation. |
| `quality_measures_period_end` | Current year-end | End date of the quality measures reporting period. |
| `record_type` | `"ip"` | CCSR record type: `"ip"` for inpatient, `"op"` for outpatient. |
| `dxccsr_version` | `"’2023.1’"` | CCSR diagnosis mapping version. |
| `prccsr_version` | `"’2023.1’"` | CCSR procedure mapping version. |

### Infrastructure & Schema

| Variable | Default | Description |
|----------|---------|-------------|
| `tuva_schema_prefix` | `None` | When set, all Tuva output schemas are prefixed with this value (e.g., `myprefix_core`). |
| `custom_bucket_name` | `"tuva-public-resources"` | Default S3 bucket for versioned seed data. Used for any database without an explicit override. |
| `tuva_seed_version` | `"1.0.0"` | Default versioned seed folder used when no per-database override is provided. Leading `v` is optional. |
| `tuva_seed_versions` | `{concept_library: "1.0.1", reference_data: "1.0.0", terminology: "1.0.0", value_sets: "1.0.0", provider_data: "1.0.0", synthetic_data: "1.0.0"}` | Optional per-database version overrides keyed by `concept_library`, `reference_data`, `terminology`, `value_sets`, `provider_data`, or `synthetic_data`. |
| `tuva_seed_buckets` | `{}` | Optional per-database bucket overrides keyed by `concept_library`, `reference_data`, `terminology`, `value_sets`, `provider_data`, or `synthetic_data`. |
| `enable_input_layer_testing` | `true` | Run DQI data quality tests on the input layer. |
| `enable_legacy_data_quality` | `false` | Build legacy (pre-DQI) data quality models. |
| `enable_normalize_engine` | `false` | Enable the normalize engine for custom code mapping. Set to `"unmapped"` to list unmapped codes, or `true` to also integrate custom mappings. |
| `provider_attribution_as_of_date` | None | Reference date for provider attribution calculations. |

## Publishing Versioned Seed Artifacts

Use `scripts/publish-dolthub-seeds` to publish the latest public DoltHub databases to versioned S3 folders.

Required inputs:
- `--version v1.0.0`
- AWS CLI credentials via `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`

Optional inputs:
- `--bucket reference_data=my-bucket`
- `--bucket value_sets=my-other-bucket/prefix`
- `--database terminology`
- `--download-only`

The script publishes to the normalized layout:
- `s3://<bucket>/<database-folder>/<version>/<table>.csv.gz`

## Mirroring Seed Releases To GCS And Azure

Use `scripts/mirror-seed-release` after an S3 publish to copy the same versioned release to GCS and Azure Blob Storage.

Required access:
- AWS CLI access to read `s3://tuva-public-resources`
- `gsutil` access to write `gs://tuva-public-resources`
- Azure `Storage Blob Data Contributor` or equivalent on storage account `tuvapublicresources`, container `tuva-public-resources`

Example:

```bash
scripts/mirror-seed-release --version v1.0.0
```

The script mirrors:
- `s3://tuva-public-resources/<database-folder>/<version>/...`
- `gs://tuva-public-resources/<database-folder>/<version>/...`
- `https://tuvapublicresources.blob.core.windows.net/tuva-public-resources/<database-folder>/<version>/...`

Current published defaults:
- `concept-library` uses `1.0.1`
- `reference-data`, `terminology`, `value-sets`, `provider-data`, and `synthetic-data` use `1.0.0`
