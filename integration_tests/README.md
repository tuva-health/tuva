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
- There is no project-local `seeds/` directory. Tuva Data Assets are defined by
  the Tuva Core package and loaded from the immutable object-storage snapshot
  matching the installed Tuva Core version.
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

`Tuva CI -- Snowflake` runs automatically for every in-repository pull request.
It tests the exact pull-request merge commit on Snowflake using Tuva Core, the
integration project, and the small synthetic dataset. Structural and Logical
Input Data Quality, Output Data Quality rollups, and the optional Logical
failure-key relation are enabled so the complete Core test surface runs. A
package version change does not change this automatic path.

For the final release pull request, dispatch `Tuva CI -- All Warehouses` from
the Actions tab and enter only its pull-request number. The workflow accepts an
open, mergeable, same-repository pull request into `main` only when its exact
test merge changes the Tuva Core package version. It resolves all eight
standalone package `main` branches once to exact commits, then uses that single
source lock for Snowflake, BigQuery, Databricks, Fabric, and Redshift.

The manual release build selects the integration project, Tuva Core, AHRQ
Quality Indicators, CCSR, CMS Chronic Conditions, CMS HCC, FHIR Preprocessing,
NYU ED Classification, Quality Measures, and Semantic Layer. Before any
warehouse job receives credentials, CI verifies that every Core asset declared
by the release candidate exists under its future version in S3, GCS, and Azure
and that `_release.json` is absent. The receipt is finalized against the merged
`main` commit later; candidate payloads alone are sufficient for PR validation.

Both workflows keep the small synthetic dataset and Data Quality failure-key
coverage enabled while parity remains disabled. The all-warehouse workflow
retains each warehouse's sanitized dbt result summary and resolved source lock
for 90 days; raw logs and warehouse connection metadata are not included. There
is no individual-warehouse dispatcher. Troubleshoot individual warehouses
locally, and use DuckDB locally for portability checks.

Reviewed fork pull requests use the separate `External PR Snowflake CI`
workflow. Its only input is the pull-request number. CI does not accept
pull-request comment commands or arbitrary dbt commands. External version
changes are rejected because the release matrix requires an internal branch.

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
