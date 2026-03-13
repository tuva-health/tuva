# Tuva Local Development Workflow

Follow this workflow for all work in this repository unless the user explicitly overrides it.

## Project Context

- The Tuva Project is an open-source healthcare data model and analytics framework built with dbt and SQL.
- It transforms raw healthcare data (claims, clinical, eligibility, pharmacy) into a standardized model used for:
  - Risk adjustment (CMS-HCC)
  - Quality measures (HEDIS)
  - Cost and utilization analytics
  - Clinical and population health analytics

## Architecture Context

- Input Layer (`models/input_layer`):
  - Defines the contract user data must conform to before package processing.
  - Users map their raw data to this contract in their own dbt project, and Tuva refs those models.
- Claims Preprocessing (`models/claims_preprocessing`):
  - Normalizes claims and runs service category and encounter grouping logic.
- Core Data Model (`models/core`):
  - Cleaned claims and clinical outputs expected to remain relatively stable.
  - Treat breaking changes carefully and communicate them clearly.
- Data Marts (`models/*` outside core/input/preprocessing):
  - Advanced marts for measures, groupers, and risk models.
- Terminology and Value Sets (`seeds/` definitions):
  - Seed definitions in repo are primarily schema/header definitions.
  - Underlying terminology/value set content is loaded from S3 via hooks.

## Local Environment Rules (Mandatory)

- Treat `integration_tests/profiles/*` as CI-only configuration. Do not use these files for local development runs.
- Treat `.github/workflows/*` as CI-only pipeline definitions, not local runbooks.
- Changes to `.github/workflows/*` are valid when the task is a CI workflow feature/fix.
- Keep `.github/workflows/create-release.yml` intact unless release workflow changes are explicitly requested.
- For docs changes under `docs/*`, run local docs build validation before push/PR.
- Use local DuckDB for development unless the user explicitly asks for MotherDuck or another warehouse.
- Run dbt from `integration_tests` using local profiles from `~/.dbt` (or `DBT_PROFILES_DIR` if set).
- Set `TUVA_DBT_PROFILE` (or `DBT_PROFILE`) for local profile selection when needed.
- Keep `integration_tests/dbt_project.yml` on `profile: default` before push.
- For data model and seed feature/bug work, validate using the shipped synthetic seed data in `integration_tests/seeds/*` on local DuckDB (do not set `use_synthetic_data: false` unless explicitly requested).

## Standard Local Docs Commands

For docs changes (`docs/**`), run:

- `cd docs && npm ci`
- `cd docs && npm run build`

## Standard Local dbt Commands

Use the repo wrapper:

- `scripts/dbt-local deps`
- `scripts/dbt-local debug`
- `scripts/dbt-local build --select <selector>`
- `scripts/dbt-local build --full-refresh --vars '{use_synthetic_data: true}'` before push for model/seed feature or bug work

Equivalent explicit form:

- `dbt --project-dir integration_tests --profiles-dir ~/.dbt <command>`

Outside contributor bridge helper:

- `scripts/outside-pr-bridge <pr-number>`

## CI Command Workflows

- Default on PR open: run `run-snowflake` (Snowflake-only `dbt run`).
- Comment commands on PRs:
  - `/ci run` runs `dbt run` across all supported warehouses.
  - `/ci run-<warehouse>` runs `dbt run` on one warehouse.
  - `/ci build` runs `dbt build --full-refresh` across all supported warehouses.
  - `/ci build-<warehouse>` runs `dbt build --full-refresh` on one warehouse.
- Supported warehouses: `snowflake`, `bigquery`, `databricks`, `fabric`, `redshift`, `duckdb`.
- If a PR changes seed/config files that require a full refresh, use a build command (`/ci build*`) instead of a run command (`/ci run*`).

## Git and PR Workflow

- Never commit directly to `main`.
- Start every new feature/bug task from a clean, up-to-date `main`:
  - Preferred: `scripts/start-dev-branch <topic>`
  - Equivalent manual flow: clean working tree -> `git fetch origin main` -> `git checkout main` -> `git pull --ff-only origin main` -> create a concise, descriptive feature branch
- Use succinct, straightforward branch names and PR titles that clearly describe the change.
- Make focused commits with clear messages.
- Push branch and open PR to `main`.
- Monitor CI checks by default after opening a PR until all required checks pass.
- If any check fails, troubleshoot, fix, push, and continue monitoring until green.
- Never merge to `main`; the user merges.

## Seed/Data Safety

- Do not modify `integration_tests/seeds/*` without explicit user approval.
- If a change appears to require seed changes, stop and ask first.
- If new synthetic columns/data are required, ask for explicit generation requirements first (for example expected date ranges).
- Seed feature/bug work in top-level `seeds/*` is allowed, but must be validated via `integration_tests` + local DuckDB before push.

## SQL Portability Rules

- Write SQL in general-purpose, cross-warehouse style.
- Tuva must run on:
  - Snowflake
  - Databricks
  - BigQuery
  - Microsoft Fabric
  - Redshift
  - DuckDB

## Build Requirements

- `dbt seed` and `dbt build` may require internet access because some seeds/value sets are loaded from public S3 resources.

## Execution Contract Per Task

For each user task:

1. Restate goal briefly and start execution.
2. Implement code changes directly.
3. Validate locally by change type and report exact commands/results:
   - docs changes: `cd docs && npm ci && npm run build`
   - data model/seed changes: run `scripts/dbt-local` commands from `integration_tests` with local DuckDB and `use_synthetic_data: true`
   - CI workflow changes: run available local workflow lint/syntax checks and confirm `/ci run|build[-warehouse]` behavior plus PR-open `run-snowflake` default
4. Push/open PR and monitor CI until required checks are green by default, then report final status.

## Outside Contributor PR Workflow

Use this for PRs that do not trigger CI because they originate outside the Tuva GitHub org.

Invocation:

```text
tuva-dev/
task: outside-pr <pr-number>
```

Deterministic flow:

1. Sync local `main` to clean `origin/main`.
2. Fetch source PR refs and prefer `pull/<pr-number>/merge` so validation runs in clean-main merge context.
3. Run local validation in DuckDB (`scripts/dbt-local deps` and `scripts/dbt-local build --full-refresh`).
4. If local validation fails, stop and report failures. Do not create bridge PR.
5. If local validation passes, push a bridge branch named `codex/outside-pr-<pr-number>-ci-<timestamp>`.
6. Open a new PR to `main` with title prefix `[outside-pr <pr-number>]` and body linking the source PR.
7. Monitor CI checks until all required checks are green; if any fail, troubleshoot/fix/push and continue until green.

Use `scripts/outside-pr-bridge <pr-number>` to execute this flow.

## Invocation Shortcut: `tuva-dev/`

If a user message starts with `tuva-dev/`, treat it as a request to run this exact local Tuva workflow end-to-end for a feature or bug task.

Preferred user format:

```text
tuva-dev/
task: <what to build or fix>
acceptance:
- <check 1>
- <check 2>
constraints:
- <optional guardrails>
```

Outside contributor format:

```text
tuva-dev/
task: outside-pr 1219
```

Fallback parsing rules:

- If only one sentence is provided after `tuva-dev/`, treat it as `task`.
- If `acceptance` is missing, propose concise acceptance criteria and proceed.
- Assume local DuckDB + `scripts/dbt-local` validation unless user says otherwise.
