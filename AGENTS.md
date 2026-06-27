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
  - Produces neutral structural, logical, and analytical data quality result
    tables.
  - Do not use `DQI` branding in this package; that belongs to Tuva Enterprise.
- Data Assets (`seeds/`):
  - Package seed CSVs are primarily definitions or headers.
  - Published terminology, value sets, provider data, and synthetic data are
    loaded from public object storage through seed hooks and explicit family
    version vars.
  - Data asset changes must preserve cross-warehouse loading behavior.

## Mandatory Local Rules

- Use local DuckDB for development unless the user explicitly asks for another
  warehouse.
- Run dbt from `integration_tests` using local profiles from `~/.dbt` or
  `DBT_PROFILES_DIR`.
- Prefer `scripts/dbt-local` for local dbt commands.
- Keep `integration_tests/dbt_project.yml` on `profile: default` before pushing.
- Treat `integration_tests/profiles/*` as CI-only configuration, not local
  runbooks.
- Treat `.github/workflows/*` as CI pipeline definitions, not local runbooks.
- Never merge to `main`; the user reviews and merges PRs.

## Seed And Data Safety

- Do not modify `integration_tests/seeds/*` without explicit user approval.
- For local validation, use the integration test defaults:
  - `use_synthetic_data: true`
  - `synthetic_data_size: small`
- For model, macro, seed, terminology, or value-set work, validate through
  `integration_tests` on local DuckDB unless the user asks for another warehouse.
- If a change requires new synthetic columns or synthetic data rows, ask for
  explicit generation requirements before editing synthetic data.
- Top-level `seeds/*` changes are allowed when requested, but must be validated
  through `integration_tests` before PR.
- Active data asset families must have explicit `tuva_seed_versions`; do not add
  fallback seed versions.

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

- Default PR CI is Snowflake-focused unless workflow configuration changes.
- Comment commands may be available for warehouse-specific or full builds.
- If a PR changes seed/config files that require a full refresh, use a
  seed-refreshing CI command before run/test-only commands.
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
