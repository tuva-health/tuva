# Tuva Core Scripts

Tuva Core keeps only scripts that are directly needed by local dbt package
development.

## Local dbt Development

Use `scripts/dbt-local` from the Tuva Core repo root to run dbt against the
`integration_tests` project:

```bash
scripts/dbt-local deps
# dbt Core v1
scripts/dbt-local parse --no-partial-parse
# dbt Core v2 or dbt Fusion
scripts/dbt-local parse
scripts/dbt-local build --full-refresh
```

The helper reads profiles from `~/.dbt` by default and chooses the `default`
profile when available. Set `DBT_PROFILES_DIR`, `TUVA_DBT_PROFILE`, or
`DBT_PROFILE` when using a different local profile location or name.

## Maintenance Utilities

Release, data-asset publishing, synthetic-data generation, and repository
workflow helper scripts live in the sibling maintenance checkout:

```text
../tuva-maintenance
```
