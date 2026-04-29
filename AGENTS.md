# Tuva Agent Operating Manual

This file is the canonical agent context for the Tuva repository. Agents must read
and follow it for all Tuva work, including work performed from separate git
worktrees. Keep workflow details here rather than duplicating them in local Codex
skills or nested agent files.

## Project Context

- The Tuva Project is an open-source healthcare data model and analytics
  framework built with dbt and SQL.
- It transforms raw healthcare data, including claims, clinical, eligibility, and
  pharmacy data, into a standardized model used for:
  - Risk adjustment, including CMS-HCC.
  - Quality measures, including HEDIS-oriented marts.
  - Cost and utilization analytics.
  - Clinical and population health analytics.
- The docs site under `docs/` is a Docusaurus project hosted by Netlify at
  `www.thetuvaproject.com`. It documents the package and includes data
  dictionaries generated from model YAML metadata.

## Architecture Context

- Input Layer (`models/input_layer`):
  - Defines the contract user data must conform to before package processing.
  - Users map raw data to this contract in their own dbt project; Tuva refs those
    input models.
- Claims Preprocessing (`models/claims_preprocessing`):
  - Normalizes claims and runs service category and encounter grouping logic.
- Core Data Model (`models/core`):
  - Cleaned claims and clinical outputs expected to remain relatively stable.
  - Treat breaking changes carefully and make them explicit in issues, PRs, and
    release labels.
- Data Marts (`models/data_marts` and other marts outside input/core/preprocessing):
  - Advanced marts for measures, groupers, risk models, analytics, and derived
    outputs.
  - When changing mart behavior or schema, update SQL, YAML descriptions/tests,
    and any docs/data dictionary source affected by the change.
- Terminology, value sets, and data assets (`seeds/`):
  - Top-level package seed CSVs are primarily definitions or headers.
  - Larger underlying terminology/value-set content is loaded from public object
    storage through seed hooks and version vars.
  - Data asset changes must preserve cross-warehouse loading behavior.

## Mandatory Local Rules

- Use local DuckDB for development unless the user explicitly asks for another
  warehouse.
- Run dbt from `integration_tests` using local profiles from `~/.dbt` or
  `DBT_PROFILES_DIR`.
- Prefer `scripts/dbt-local` for local dbt commands.
- Set `TUVA_DBT_PROFILE` or `DBT_PROFILE` only when local profile selection is
  needed.
- Keep `integration_tests/dbt_project.yml` on `profile: default` before pushing.
- Treat `integration_tests/profiles/*` as CI-only configuration, not local
  runbooks.
- Treat `.github/workflows/*` as CI pipeline definitions, not local runbooks.
- Keep `.github/workflows/create-release.yml` intact unless the task explicitly
  targets release automation.
- Never merge to `main`; the user reviews and merges PRs.

## Seed And Data Safety

- Do not modify `integration_tests/seeds/*` without explicit user approval.
- For local validation, use the integration test defaults:
  - `use_synthetic_data: true`
  - `synthetic_data_size: small`
- For data model, data mart, seed, terminology, or value-set work, validate
  against the shipped synthetic data on local DuckDB.
- If a change appears to require new synthetic columns or synthetic data rows,
  stop and ask for explicit generation requirements before editing
  `integration_tests/seeds/*`.
- Seed feature or bug work in top-level `seeds/*` is allowed when requested, but
  must be validated through `integration_tests` and local DuckDB before PR.

## Issue-First Worktree Workflow

Use this workflow for normal Tuva issue work, especially when the user points to a
GitHub issue or asks the agent to create one.

1. Issue creation:
   - If the user asks the agent to create an issue, create it in
     `tuva-health/tuva`.
   - Include title, problem statement, acceptance criteria, constraints,
     validation expectations, and one release-note label.
   - If acceptance criteria or the release-note label are ambiguous, ask the
     user. If a conservative default is clear, choose it and record the
     assumption in the issue.
2. Planning:
   - Read the issue body and comments.
   - Inspect the relevant repo context.
   - Produce a concise implementation plan before creating a worktree or making
     code changes.
3. Branch and worktree:
   - After the plan is accepted, run `scripts/start-issue-worktree <issue-number>
     [slug]`.
   - The helper creates or reuses a GitHub-linked branch named
     `codex/issue-<n>-<slug>` from `origin/main`.
   - The helper creates or reuses a local worktree under
     `/Users/aaronneiderhiser/code/tuva-worktrees/issue-<n>-<slug>`.
   - All implementation happens in that worktree. Do not reuse the main checkout
     for issue implementation.
4. Implementation:
   - Keep changes focused on the issue acceptance criteria.
   - Preserve cross-warehouse SQL portability.
   - Update docs and YAML metadata when model behavior, schema, or user-facing
     fields change.
5. PR:
   - Rebase or update from `origin/main` before pushing when needed.
   - Push the linked branch and open a PR to `main`.
   - The PR body must include summary, validation, release-note label, and
     `Closes #<issue-number>`.
   - Mirror the issue release-note label onto the PR.
6. CI:
   - PR open still triggers the default Snowflake `dbt run`.
   - After PR creation, post `/ci snowflake dbt seed dbt run`.
   - Monitor checks, fix failures in the same worktree, push, and continue until
     the relevant checks pass.
   - Leave merge to the user.

`scripts/start-dev-branch <topic>` remains available for older non-issue flows,
but issue-linked worktrees are preferred.

## Release Labels And Notes

Use exactly one release-note disposition on each issue and PR:

- `breaking-change`
- `enhancement`
- `bug`
- `docs`
- `terminology`
- `connector`
- `ignore-for-release`

Optional version labels such as `v0.18.0` may be used for triage, but they do not
drive release note categories.

Release behavior:

- `.github/workflows/create-release.yml` creates a draft release when the package
  version changes in `dbt_project.yml` on `main`.
- GitHub generated release notes are built from merged PRs between tags.
- `.github/release.yml` groups those notes by PR labels.
- Because release notes are PR-label based, mirror the issue release-note label
  onto the PR.
- Use `Closes #<issue-number>` in PR bodies so merge closes the issue and keeps
  issue-to-release traceability.

## Local Validation

Standard dbt commands:

- `scripts/dbt-local deps`
- `scripts/dbt-local debug`
- `scripts/dbt-local build --select <selector>`
- `scripts/dbt-local build --full-refresh`

Validation expectations by change type:

- Docs changes under `docs/**`:
  - `cd docs && npm ci`
  - `cd docs && npm run build`
- Models, marts, macros, seeds, terminology, or value sets:
  - `scripts/dbt-local deps`
  - Run the narrowest useful `scripts/dbt-local build --select <selector>`.
  - Run `scripts/dbt-local build --full-refresh` before PR.
- CI workflow changes:
  - Validate workflow YAML and available local workflow linting tools.
  - Confirm PR-open default remains `run-snowflake`.
  - Confirm `/ci run|build[-warehouse]` mappings still behave as documented.

`dbt seed`, `dbt run`, and `dbt build` may require internet access because some
seed and value-set content is loaded from public object storage.

## CI Command Workflows

- Default on PR open: Snowflake-only `dbt run`.
- Comment commands on PRs:
  - `/ci run` runs `dbt run` across all supported warehouses.
  - `/ci run-<warehouse>` runs `dbt run` on one warehouse.
  - `/ci build` runs `dbt build --full-refresh` across all supported warehouses.
  - `/ci build-<warehouse>` runs `dbt build --full-refresh` on one warehouse.
  - `/ci snowflake dbt seed dbt run` runs Snowflake seed followed by Snowflake run.
- Supported warehouses: `snowflake`, `bigquery`, `databricks`, `fabric`,
  `redshift`, `duckdb`.
- If a PR changes seed/config files that require a full refresh, use a
  seed-refreshing CI command before run/test-only commands.

## SQL Portability Rules

- Write SQL in general-purpose, cross-warehouse style.
- Tuva must run on:
  - Snowflake
  - Databricks
  - BigQuery
  - Microsoft Fabric
  - Redshift
  - DuckDB
- Prefer existing Tuva macros and package patterns over warehouse-specific SQL.

## Outside Contributor PR Workflow

Use this for PRs that do not trigger CI because they originate outside the Tuva
GitHub org.

Invocation:

```text
tuva-dev/
task: outside-pr <pr-number>
```

Deterministic flow:

1. Sync local `main` to clean `origin/main`.
2. Fetch source PR refs and prefer `pull/<pr-number>/merge` so validation runs
   in clean-main merge context.
3. Run local validation in DuckDB with `scripts/dbt-local deps` and
   `scripts/dbt-local build --full-refresh`.
4. If local validation fails, stop and report failures. Do not create a bridge PR.
5. If local validation passes, push a bridge branch named
   `codex/outside-pr-<pr-number>-ci-<timestamp>`.
6. Open a new PR to `main` with title prefix `[outside-pr <pr-number>]` and body
   linking the source PR.
7. Monitor CI checks until all required checks are green; if any fail,
   troubleshoot, fix, push, and continue until green.

Use `scripts/outside-pr-bridge <pr-number>` to execute this flow.

## Invocation Shortcut: `tuva-dev/`

If a user message starts with `tuva-dev/`, treat it as a request to run this Tuva
workflow for a feature, bug, issue, or outside PR task.

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

Fallback parsing rules:

- If only one sentence is provided after `tuva-dev/`, treat it as the task.
- If the task references an existing issue, follow the issue-first worktree
  workflow.
- If acceptance is missing and no issue exists, infer concise acceptance criteria
  and record them in the issue or PR.

## Output Contract

For each task, report:

- Issue and PR URLs when created.
- Branch and worktree path.
- What changed and why.
- Local validation commands run and key outcomes.
- CI commands/check status when run.
- Any blockers, assumptions, or required user decisions.
