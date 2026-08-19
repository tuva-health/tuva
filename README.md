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

`dbt seed` and `dbt build` load Tuva Data Assets from the immutable public
object-storage snapshot that matches the installed Tuva Core version. For
example, Tuva Core `1.0.0` loads from `tuva-core/1.0.0/`. `dbt run` assumes
those relations already exist, so on a fresh database run `seed` or `build`
first.

Once the data assets are loaded, iterate with:

```bash
./scripts/dbt-local run
```

## dbt Variables

Set Tuva vars under the `vars:` key in your dbt project's `dbt_project.yml`.

Tuva Core derives its data-asset version from the installed package version,
so projects do not maintain separate terminology, value-set, provider-data, or
synthetic-data version pins. The most complete commented example for
development lives in `integration_tests/dbt_project.yml`; the public docs
reference is maintained at
[thetuvaproject.com/dbt-variables](https://www.thetuvaproject.com/dbt-variables).

Common variable groups:

- Domain enablement: `claims_enabled`, `clinical_enabled`, `provider_attribution_enabled`
- Data quality: `data_quality_enabled`
- Data assets: `custom_bucket_name`, `tuva_seed_buckets`
- Synthetic data validation: `use_synthetic_data`, `synthetic_data_size`
- Runtime metadata and schemas: `tuva_last_run`, `tuva_schema_prefix`
- Extension columns: `passthrough`

## Data Asset Releases

Every Tuva Core release with external data assets has one complete immutable
snapshot at:

```text
tuva-core/<package-version>/
├── terminology/
├── provider-data/
├── synthetic-data/
│   ├── small/
│   └── large/
└── value-sets/
```

Physical object names exactly match their dbt seed resource names and end in
`.csv.gz`. `data_assets.yml` is the package-owned inventory used by the release
publisher. Each storage location receives `_release.json` only after all of its
payload objects verify. The receipt binds that snapshot to the exact
40-character lowercase git SHA in `package_commit`; dbt loaders do not read the
receipt. The release workflow will not create the package tag unless
`package_commit` equals the current `main` commit and byte-identical receipts
are publicly available from S3, GCS, and Azure. If the automatic version-bump
run reaches the gate before assets are published, maintainers can recover with
the main-only manual dispatch after publication.

Published CSVs use one cross-warehouse null contract: all empty values and
unquoted `\N`/`\\N` markers become bare empty fields (loaded as null), while
quoted marker strings remain literal text. The publisher rejects the reserved
loader sentinel `__TUVA_RESERVED_NULL_MARKER_1_0__` and the NUL control
character. NUL is reserved so Athena can disable OpenCSV's backslash escape and
preserve quoted marker strings.

Standalone packages can use the shared loader while deriving the installed
version from dbt rather than duplicating it in configuration:

```jinja
{{ the_tuva_project.load_package_seed(
    'cms_hcc',
    'cms-hcc',
    'cms_hcc__sample.csv.gz'
) }}
```

The first argument is the dbt package name, the second is its stable storage
slug, and the third is the object path within that package-version snapshot.
`custom_bucket_name` overrides the default public bucket. The
`tuva_seed_buckets` mapping can override an individual Core family or package
slug while preserving the package/version/object path contract.

Tuva Core 1.0 removes independent family-version configuration and the
`get_seed_version` macro. The deprecated positional version arguments on
`get_versioned_seed_uri`, `load_versioned_seed`, and
`load_versioned_synthetic_seed` now raise a compiler error instead of selecting
a release that can drift from the installed package.

Redshift loading uses `IAM_ROLE default`; configure the cluster or serverless
namespace with a default IAM role that can read the selected data-asset bucket.
RDS PostgreSQL loading likewise requires its database IAM role to read that S3
bucket. Released gzip objects carry `Content-Encoding: gzip` so the RDS
`aws_s3` extension can decompress them.

## Maintainer Scripts

General local development should use `scripts/dbt-local`. The package-owned
asset inventory is declared in `data_assets.yml`. Release, data-asset
publishing, synthetic-data generation, and repository-maintenance helper
scripts live in the sibling `tuva-maintenance` checkout. The generated
`_release.json` receipt is written to object storage by the publisher and is
not read by dbt at runtime.

## Agentic Workflow

If you are using coding agents in this repo, the local workflow guidance lives in [AGENTS.md](AGENTS.md).

## License

Tuva Core is released under the [Apache 2.0 License](license/license-2.0.txt).
