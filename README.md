[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.10%2B&color=orange)

![Tuva Project Overview](./docs/static/img/tuva_project_overview_from_downloads.jpg)

## What is the Tuva Project?

The Tuva Project is a dbt package for transforming raw healthcare data into analytics-ready data. The package includes:

- Input Layer
- Claims Preprocessing
- Core Data Model
- Data Marts
- Terminology and Value Sets

## Docs

- [Getting Started](https://www.thetuvaproject.com/getting-started)
- [dbt Variables](https://www.thetuvaproject.com/dbt-variables)
- [Full Documentation](https://www.thetuvaproject.com/)

The docs source for the getting-started runbook lives in [docs/docs/getting-started.md](./docs/docs/getting-started.md).

## Local Development

Recommended local setup:

- Python 3.10 or later
- DuckDB
- `dbt-core` and `dbt-duckdb`

Use `integration_tests` as your development project. Configure a DuckDB connection in `profiles.yml`, then run from the repo root:

```bash
./scripts/dbt-local deps
./scripts/dbt-local build --full-refresh
```

`dbt seed` and `dbt build` load synthetic data into `raw_data` from versioned S3 artifacts. `dbt run` assumes those relations already exist, so on a fresh database you should run `seed` or `build` first.

Once the synthetic data is loaded, iterate with:

```bash
./scripts/dbt-local run
```

## Agentic Workflow

If you are using coding agents in this repo, the local workflow guidance lives in [AGENTS.md](AGENTS.md).

## dbt Variables

Set Tuva vars under the `vars:` key in your `dbt_project.yml`. Use dbt selectors to run individual marts; the vars below control broad data domains, shared runtime behavior, and the synthetic bootstrap flow used by `integration_tests`.

### Broad Enablement

| Variable | Root Default | `integration_tests` Default | Description |
|----------|--------------|-----------------------------|-------------|
| `claims_enabled` | `false` | `true` | Enable claims-based models. |
| `clinical_enabled` | `false` | `true` | Enable clinical-based models. |
| `provider_attribution_enabled` | `false` | `true` | Enable provider attribution models. Claims input must also be enabled. |
| `semantic_layer_enabled` | `false` | `true` | Enable semantic-layer models. Claims-dependent semantic models also require `claims_enabled`. |
| `fhir_preprocessing_enabled` | `false` | `false` | Enable FHIR preprocessing models. |

### Shared Runtime Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `tuva_last_run` | Current UTC timestamp | Populates the `tuva_last_run` column in output models. |
| `tuva_schema_prefix` | unset | Prefixes output schemas, for example `myprefix_core`. |
| `cms_hcc_payment_year` | Current year | CMS-HCC payment year used for risk scoring. |
| `quality_measures_period_end` | Current year-end | Optional reporting-period end date for quality measures. |
| `record_type` | `"ip"` | CCSR record type: `"ip"` for inpatient or `"op"` for outpatient. |
| `dxccsr_version` | `"2023.1"` | CCSR diagnosis mapping version. |
| `prccsr_version` | `"2023.1"` | CCSR procedure mapping version. |
| `provider_attribution_as_of_date` | unset | Optional `YYYY-MM-DD` override for provider attribution current-state calculations. |

### Seed And Feature Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `custom_bucket_name` | `"tuva-public-resources"` | Default bucket for versioned Tuva seed artifacts. |
| `tuva_seed_version` | `"0.18.0"` | Versioned seed release to load. Leading `v` is optional. |
| `tuva_seed_buckets` | `{}` | Optional per-database bucket overrides for `concept_library`, `reference_data`, `terminology`, `value_sets`, `provider_data`, or `synthetic_data`. |
| `synthetic_data_size` | `small` in `integration_tests` | Selects the `small` or `large` synthetic input payload when running `integration_tests`. |
| `enable_input_layer_testing` | `true` | Runs DQI checks on the input layer. |
| `enable_legacy_data_quality` | `false` | Builds the legacy pre-DQI data-quality models. |
| `enable_normalize_engine` | `false` | Set to `unmapped` to surface unmapped code models, or `true` to also use custom mappings. |

See the maintained docs reference at [thetuvaproject.com/dbt-variables](https://www.thetuvaproject.com/dbt-variables) for examples and more detail.

## Publishing Versioned Seed Artifacts

Use `scripts/publish-dolthub-seeds` to publish the latest public DoltHub databases to versioned S3 folders.

Required inputs:
- `--version v0.18.0`
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
scripts/mirror-seed-release --version v0.18.0
```

The script mirrors:
- `s3://tuva-public-resources/<database-folder>/<version>/...`
- `gs://tuva-public-resources/<database-folder>/<version>/...`
- `https://tuvapublicresources.blob.core.windows.net/tuva-public-resources/<database-folder>/<version>/...`
