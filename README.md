# Tuva Core

[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.10%2B&color=orange)

Tuva Core is the dbt package that aggregates and transforms payer claims, EHR, and other healthcare data into a common analytics- and AI-ready data model inside your cloud data warehouse.

The package includes:

- Input Layer contracts for payer and provider source data
- Normalized Layer models
- Claims Preprocessing models
- Core Data Model tables
- Neutral data quality result tables
- Tuva Data Assets, including terminology, value sets, provider data, and synthetic data

## Documentation

- [Getting Started](https://www.thetuvaproject.com/getting-started)
- [dbt Variables](https://www.thetuvaproject.com/dbt-variables)
- [Full Documentation](https://www.thetuvaproject.com/)

The docs site and DAG Viewer live in separate sibling repositories. During local development they read dbt, YAML, and SQL metadata from this Tuva Core checkout through `TUVA_CORE_PATH`.

Example local validation:

```bash
cd ../docs
TUVA_CORE_PATH=../tuva-core npm run build

cd ../dag-viewer
TUVA_CORE_PATH=../tuva-core npm run build:local
```

## Local Development

Recommended local setup:

- Python 3.10 or later
- DuckDB
- `dbt-core` and `dbt-duckdb`

Use `integration_tests` as the local development project. It maps Tuva synthetic data into the Input Layer, imports the local package, and exercises the Tuva Core path.

Create or update `~/.dbt/profiles.yml` with a local DuckDB profile. For DuckDB seed stability, use one thread:

```yaml
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /tmp/tuva_core.duckdb
      threads: 1
```

Run dbt from the repo root with the helper script:

```bash
./scripts/dbt-local deps
./scripts/dbt-local build --full-refresh
```

`dbt seed` and `dbt build` load Tuva Data Assets from versioned public object-storage releases. `dbt run` assumes those relations already exist, so on a fresh database run `seed` or `build` first.

Once the data assets are loaded, iterate with:

```bash
./scripts/dbt-local run
```

## dbt Variables

Set Tuva vars under the `vars:` key in your dbt project's `dbt_project.yml`.

Tuva Core keeps root package defaults focused on explicit data asset family versions and run metadata. The most complete commented example for development lives in `integration_tests/dbt_project.yml`; the public docs reference is maintained at [thetuvaproject.com/dbt-variables](https://www.thetuvaproject.com/dbt-variables).

Common variable groups:

- Domain enablement: `claims_enabled`, `clinical_enabled`, `provider_attribution_enabled`
- Data quality: `data_quality_enabled`
- Data assets: `custom_bucket_name`, `tuva_seed_versions`, `tuva_seed_buckets`
- Synthetic data validation: `use_synthetic_data`, `synthetic_data_size`
- Runtime metadata and schemas: `tuva_last_run`, `tuva_schema_prefix`
- Extension columns: `passthrough`

## Maintainer Scripts

General local development should use `scripts/dbt-local`. Release, data-asset publishing, synthetic-data generation, and repository-maintenance helper scripts live in the sibling `tuva-maintenance` checkout.

## Agentic Workflow

If you are using coding agents in this repo, the local workflow guidance lives in [AGENTS.md](AGENTS.md).

## License

Tuva Core is released under the [Apache 2.0 License](license/license-2.0.txt).
