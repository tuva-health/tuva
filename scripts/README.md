# Tuva Core Scripts

Tuva Core keeps only scripts that are directly needed by the dbt package
development and CI command flow.

## Local dbt Development

Use `scripts/dbt-local` from the Tuva Core repo root to run dbt against the
`integration_tests` project:

```bash
scripts/dbt-local deps
scripts/dbt-local parse --no-partial-parse
scripts/dbt-local build --full-refresh
```

The helper reads profiles from `~/.dbt` by default and chooses the `default`
profile when available. Set `DBT_PROFILES_DIR`, `TUVA_DBT_PROFILE`, or
`DBT_PROFILE` when using a different local profile location or name.

## CI Command Parsing

`parse_ci_command.py` is used by GitHub Actions to parse and authorize `/ci`
pull request comments. Its unit tests live next to it:

```bash
python -m unittest scripts/test_parse_ci_command.py
```

## Maintenance Utilities

Release, data-asset publishing, synthetic-data generation, and repository
workflow helper scripts live in the sibling maintenance checkout:

```text
../tuva-maintenance
```
