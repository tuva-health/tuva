# Tuva Core Integration Tests

`integration_tests` is the dbt project used for local development and CI
validation of Tuva Core. It imports the local Tuva Core package, maps Tuva
synthetic data assets into Input Layer tables, and then runs the package against
those inputs.

## Folder Layout

- `models/`: synthetic Input Layer mappings. These models ref Tuva Core
  synthetic seeds and materialize the payer and provider Input Layer tables.
- `tests/`: integration-only regression tests that validate behavior across the
  package boundary.
- `macros/`: integration and CI helper macros.
- `seeds/`: intentionally empty. Tuva Data Assets are defined by the Tuva Core
  package and loaded from the immutable object-storage snapshot matching the
  installed Tuva Core version.
- `profiles/`: CI-only warehouse profiles. Use your local `~/.dbt/profiles.yml`
  for development.

## Local Setup

Create or update `~/.dbt/profiles.yml` with a local profile. For DuckDB, use one
thread for seed-load stability:

```yaml
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /tmp/tuva_core.duckdb
      threads: 1
```

Run commands from the Tuva Core repo root with the helper script:

```bash
scripts/dbt-local deps
scripts/dbt-local build --full-refresh --select package:integration_tests package:the_tuva_project
```

`scripts/dbt-local` automatically points dbt at this `integration_tests` project
and reads profiles from `~/.dbt` unless `DBT_PROFILES_DIR`,
`TUVA_DBT_PROFILE`, or `DBT_PROFILE` is set.

## Synthetic Data

Synthetic data is controlled through vars in `integration_tests/dbt_project.yml`.
The defaults are:

- `use_synthetic_data: true`
- `synthetic_data_size: small`

Use the large synthetic data release for heavier validation:

```bash
scripts/dbt-local build --full-refresh \
  --select package:integration_tests package:the_tuva_project \
  --vars '{synthetic_data_size: large}'
```

Supported data-asset bucket overrides and other vars are documented in
`integration_tests/dbt_project.yml`. Asset versions are not configured
separately; they always match the installed Tuva Core package version. Treat
that file as the canonical commented example for local development.

## Tuva Core Validation

This integration project installs only the local Tuva Core package. Select the
integration-test project and Tuva Core explicitly:

```bash
scripts/dbt-local build --full-refresh --select package:integration_tests package:the_tuva_project
```

Standalone packages own their integration tests in their respective
repositories. Validate those packages against a local Tuva Core checkout when
testing cross-package compatibility.

## Continuous Integration

In-repository pull requests automatically run the Core build on Snowflake with
the small synthetic dataset. If the pull request changes the top-level Tuva Core
version, that run automatically switches to release validation: the exact pull
request test-merge commit plus all eight accepted standalone package `main`
branches run on Snowflake, BigQuery, Databricks, Fabric, and Redshift. Package
branches are resolved once to exact commits so every warehouse tests the same
source set. DuckDB remains available for local portability checks.

Release CI selects the integration project, Tuva Core, AHRQ Quality Indicators,
CCSR, CMS Chronic Conditions, CMS HCC, FHIR Preprocessing, NYU ED
Classification, Quality Measures, and Semantic Layer. It keeps the small
synthetic dataset enabled and parity disabled, and retains each warehouse's dbt
result summary and resolved source lock for 90 days. Raw logs and warehouse
connection metadata are not included. There is no general-purpose manual
warehouse dispatcher; troubleshoot individual warehouses locally.

Reviewed fork pull requests use the separate `External PR Snowflake CI`
workflow. Its only input is the pull-request number. CI does not accept
pull-request comment commands or arbitrary dbt commands. Version changes must
come from an internal branch so the secrets-backed release matrix can run.

## Data Asset Loading

`dbt seed` and `dbt build` load Tuva Data Assets, including synthetic input data,
from `tuva-core/<installed-package-version>/`. Synthetic objects are selected
from the `synthetic-data/small/` or `synthetic-data/large/` subfolder. `dbt run`
assumes those seed relations already exist, so run `dbt seed` or `dbt build`
first on a fresh database.

## Fabric Note

For Microsoft Fabric validation, use `--cache-selected-only` with core-only
selectors. This avoids dbt-fabric relation-cache inspection of unselected
package schemas.

Example:

```bash
dbt build \
  --project-dir integration_tests \
  --profiles-dir ~/.dbt \
  --profile fabric \
  --target dev \
  --full-refresh \
  --threads 1 \
  --cache-selected-only \
  --select package:integration_tests package:the_tuva_project \
  --vars '{synthetic_data_size: small}'
```
