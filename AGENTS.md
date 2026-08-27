# Tuva Core Agent Operating Manual

This file is the canonical agent context for the Tuva Core dbt package. Agents
must read and follow it before making Tuva Core changes, including work performed
from git worktrees.

## Project Context

- Tuva Core is the dbt package that transforms payer claims, clinical, and other
  healthcare data into the Tuva common data model.
- The dbt project name remains `the_tuva_project` for dbt package compatibility,
  even though the repository is `tuva-core`.
- Docs live in the separate sibling `docs` repository. During local development,
  docs read this checkout through `TUVA_CORE_PATH`.
- The DAG Viewer lives in the separate sibling `dag-viewer` repository. During
  local development, it reads this checkout through `TUVA_CORE_PATH`.
- Advanced data marts live in separate dbt package repositories that depend on
  Tuva Core. Tuva Core should not own those mart models, seeds, tests, or docs.

## Architecture Context

- Input Layer (`models/input_layer`):
  - Defines the contract users map source data into before running Tuva Core.
  - Input Layer models are refs supplied by the parent project.
- Normalized Layer (`models/normalized_layer`):
  - Casts and standardizes Input Layer records.
  - Applies terminology normalization and creates normalized outputs used by
    Claims Preprocessing and Core.
- Claims Preprocessing (`models/claims_preprocessing`):
  - Builds reusable claims-derived intermediate products such as member month,
    claims enrollment flags, service categories, encounters, and attribution.
- Core Data Model (`models/core`):
  - Produces the public Tuva Core tables expected to remain relatively stable.
  - Treat schema, grain, and semantic changes as breaking unless clearly proven
    otherwise.
- Data Quality (`models/data_quality`):
  - Produces neutral Input Data Quality results (Structural and Logical) and
    Output Data Quality rollups.
  - Do not use `DQI` branding in this package; that belongs to Tuva Enterprise.
- Data Assets (`seeds/`):
  - Package seed CSVs are primarily definitions or headers.
  - Published terminology, value sets, provider data, and synthetic data are
    loaded from the immutable public object-storage snapshot matching the Tuva
    Core package version.
  - `data_assets.yml` declares the exact package-owned object inventory. The
    publisher-generated `_release.json` is a completion receipt bound to the
    exact package git commit in `package_commit` and is not read by dbt at
    runtime. Future-version payload folders may be created before the matching
    version change reaches `main`, but they are candidates rather than completed
    snapshots and must not contain `_release.json`. Package tags must not be
    created until the post-merge receipt is available from S3, GCS, and Azure
    and names the current `main` commit.
  - Data asset changes must preserve cross-warehouse loading behavior.

## Mandatory Local Rules

- For routine Tuva Core model, test, or YAML validation, use the `snowflake-dev`
  profile against the existing `dev_aaron` database. Reuse its loaded seed and
  data-asset relations when seed definitions, versions, schemas, and loading
  logic are unchanged.
- Do not run `dbt seed` or select seed nodes during that routine validation.
  Reload seeds only when seed or data-asset inputs/loading behavior changed, or
  when the user explicitly requests a seed refresh.
- Use local DuckDB when specifically validating DuckDB portability or when the
  task does not require the large published seed assets.
- Run dbt from `integration_tests` using local profiles from `~/.dbt` or
  `DBT_PROFILES_DIR`.
- Prefer `scripts/dbt-local` for local dbt commands.
- Keep `integration_tests/dbt_project.yml` on `profile: default` before pushing.
- Treat `integration_tests/profiles/*` as CI-only configuration, not local
  runbooks.
- Treat `.github/workflows/*` as CI pipeline definitions, not local runbooks.
- Never merge to `main` without Aaron's explicit approval.

## Seed And Data Safety

- Do not modify `integration_tests/seeds/*` without explicit user approval.
- For local validation, use the integration test defaults:
  - `use_synthetic_data: true`
  - `synthetic_data_size: small`
- For model, macro, test, or YAML work that does not change seeds, validate
  through `integration_tests` with `TUVA_DBT_PROFILE=snowflake-dev` and reuse
  the seed relations already loaded in `dev_aaron`.
- If a change requires new synthetic columns or synthetic data rows, ask for
  explicit generation requirements before editing synthetic data.
- Top-level `seeds/*` changes are allowed when requested, but must be validated
  through `integration_tests` before PR.
- Do not add independent data-asset family versions. Every seed resolves
  through the installed Tuva Core package version.

## Generated Artifacts

Do not commit generated or local-only artifacts, including:

- `target/`
- `dbt_packages/`
- `logs/`
- local DuckDB database files
- docs or DAG Viewer build output
- `node_modules/`
- temporary scratch folders

## Local Validation

Standard dbt commands:

- `scripts/dbt-local deps`
- `scripts/dbt-local debug`
- `scripts/dbt-local parse --no-partial-parse`
- `scripts/dbt-local build --select <selector>`
- `scripts/dbt-local build --full-refresh`

Validation expectations by change type:

- Tuva Core model, macro, seed, test, or YAML changes:
  - Run `scripts/dbt-local deps` when dependencies may have changed.
  - Run the narrowest useful `scripts/dbt-local build --select <selector>`.
  - When seeds are unchanged, use `TUVA_DBT_PROFILE=snowflake-dev` and exclude
    seed nodes rather than reloading them.
  - Run `scripts/dbt-local build --full-refresh` before PR when feasible.
- Data asset publisher or metadata changes:
  - Work in the sibling `tuva-maintenance` checkout and run the relevant script tests there.
  - Run at least one seed smoke test against local DuckDB.
- Docs changes:
  - Use the sibling `docs` repository.
  - Set `TUVA_CORE_PATH=/path/to/tuva-core`.
  - Run `cd ../docs && npm ci && npm run build`.
- DAG Viewer changes:
  - Use the sibling `dag-viewer` repository.
  - Set `TUVA_CORE_PATH=/path/to/tuva-core`.
  - Run `cd ../dag-viewer && npm ci && npm run build:local`.
- Data mart changes:
  - Work in the relevant data mart repository.
  - Validate that package against the local Tuva Core checkout when the mart
    supports local package overrides.

`dbt seed`, `dbt run`, and `dbt build` may require internet access because seed
content is loaded from public object storage.

## SQL Portability Rules

- Write SQL in general-purpose, cross-warehouse style.
- Tuva Core must run on:
  - Snowflake
  - Databricks
  - BigQuery
  - Microsoft Fabric
  - Redshift
  - DuckDB
- Prefer existing Tuva macros and package patterns over warehouse-specific SQL.
- Keep warehouse-specific logic isolated behind dispatched macros when needed.

## GitHub And PR Guidance

- If creating an issue, create it in `tuva-health/tuva-core`.
- Use exactly one release-note disposition on each issue and PR:
  - `breaking-change`
  - `enhancement`
  - `bug`
  - `docs`
  - `terminology`
  - `connector`
  - `ignore-for-release`
- PR bodies should include summary, validation, release-note label, and linked
  issue when applicable.
- Mirror the issue release-note label onto the PR.
- Leave merge to the user.

## CI Guidance

- In-repository pull requests automatically run `Tuva CI -- Snowflake`: one
  Snowflake `dbt build --full-refresh` against the small synthetic dataset. The
  build selects Tuva Core plus the integration-test project and runs unit and
  data tests. Data Quality, including its optional failure-key relation, is
  enabled so the complete Core test surface executes. Version changes do not
  alter this automatic path. Parity remains disabled.
- Run `Tuva CI -- All Warehouses` manually from the Actions tab for the final
  release pull request. Its only input is the pull-request number. It accepts
  only an open, mergeable, same-repository pull request into `main` that changes
  the Tuva Core package version.
- The all-warehouse workflow resolves the pull request test-merge and all eight
  standalone package `main` branches once to exact commits. It then runs Tuva
  Core, the integration-test project, and all eight packages on Snowflake,
  BigQuery, Databricks, Fabric, and Redshift against synthetic small with Data
  Quality and its optional failure-key relation enabled. Before warehouse
  credentials are used, it verifies that every Core candidate asset exists in
  S3, GCS, and Azure and that no `_release.json` has been finalized.
- CI does not expose individual warehouse dispatches. Troubleshoot a single
  warehouse locally when needed.
- DuckDB portability is validated locally as needed rather than in GitHub CI.
- Pull-request comment commands do not trigger CI and CI does not accept
  arbitrary dbt commands, selectors, or flags.
- Automatic secrets-backed CI does not execute fork code. After reviewing an
  external pull request, a maintainer runs `External PR Snowflake CI` from the
  Actions tab and supplies only the pull-request number. External version
  changes are rejected because release CI requires a same-repository branch.
- Standalone packages validate in their own repositories. Routine Snowflake CI
  does not install standalone packages; only manual release CI snapshots their
  `main` branches to exact commits before building.
- Parity comparisons are separate, manually initiated Snowflake validations.
- Do not edit `.github/workflows/create-release.yml` unless the task explicitly
  targets release automation.

## Output Contract

For each task, report:

- What changed and why.
- Local validation commands run and key outcomes.
- Issue and PR URLs when created.
- Branch and worktree path when relevant.
- CI commands/check status when run.
- Any blockers, assumptions, or required user decisions.
