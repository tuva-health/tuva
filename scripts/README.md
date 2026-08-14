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

## Model Conventions

`check_model_conventions.py` is the specification for model structure and
naming, and enforces it. The rules are documented at the top of that file. It
reads the model files directly, so it needs no warehouse, no credentials and no
manifest, and runs as its own CI job on every pull request:

```bash
python3 scripts/check_model_conventions.py          # check; exit 1 on any violation
python3 scripts/check_model_conventions.py --list   # print the rule table
```

Rules are registered, not hard-coded into a walker. Adding one is a single
decorated function -- no existing rule is touched:

```python
@model_rule("name-suffix", "published models do not end in _v1")
def check_no_version_suffix(m):
    if m.tier == "final" and m.name.endswith("_v1"):
        yield m.problem("published model names carry no version suffix")
```

`@model_rule` sees one .sql model, `@yaml_rule` one .yml file, `@project_rule`
every model at once for cross-file checks. Vocabulary and structure live in the
CONFIG block, so adding a module or renaming a term is a data edit.

Every rule carries unit tests in both directions -- a case that passes and a
case that must be caught. This matters more than it sounds: the checker had
shipped three false negatives, and a checker that reports OK while violations
pass through is worse than none. Those three shapes are now regression tests.

```bash
python -m unittest discover -s scripts -p "test_*.py"
```

It fails on a model name whose module is outside the closed set or disagrees
with its folder, a stage marker that disagrees with its tier folder, banned
vocabulary (`asc`, `snf`, `psych`, `prof`, `ptotst`), a hand-written `alias` or
a `schema:` that does not point at its file's anchor, and any `alias`, `schema`,
`tags` or `materialized` in a SQL `config()` block. Relation names are derived
by `macros/generate_alias_name.sql`; a single hand-written `alias` silently
defeats it. YAML owns model config, and only `enabled` may stay in a SQL
`config()`: scalars declared in both places silently override, and tags
declared in both places merge into duplicates.

## CI Command Parsing

`parse_ci_command.py` is used by GitHub Actions to parse and authorize `/ci`
pull request comments. The default `/ci` command runs Snowflake Tuva Core seed
and run. `/ci build` runs Tuva Core build across active warehouses, and
`/ci marts` runs Snowflake seed and run across Tuva Core plus external data mart
packages. Its unit tests live next to it:

```bash
python -m unittest scripts/test_parse_ci_command.py
```

## Maintenance Utilities

Release, data-asset publishing, synthetic-data generation, and repository
workflow helper scripts live in the sibling maintenance checkout:

```text
../tuva-maintenance
```
