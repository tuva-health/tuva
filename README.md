# Tuva Core

[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![dbt 1.10.5 through 2.x](https://img.shields.io/static/v1?logo=dbt&label=dbt&message=1.10.5%20to%202.x&color=orange)

Tuva Core is the dbt package that transforms claims, clinical, and other
healthcare data into a common analytics- and AI-ready data model inside your
cloud data warehouse.

The repository is `tuva-health/tuva-core`, while the dbt project name remains
`the_tuva_project` for package compatibility. The `main` branch currently
declares the Tuva Core 1.0.0 contract, but a version on `main` is not a formal
release. Production projects should use an immutable version published in
[GitHub Releases](https://github.com/tuva-health/tuva-core/releases).

Tuva Core requires dbt 1.10.5 through 2.x and supports Snowflake, Databricks,
BigQuery, Microsoft Fabric, Redshift, and DuckDB. The 1.0 package ecosystem is
validated against both dbt Core 2.0 and dbt Fusion on DuckDB.

## What Tuva Core Includes

| Area | Responsibility |
| --- | --- |
| Input Layer | Contracts that connector and parent projects map source data into |
| Normalized Layer | Portable type casting, reshaping, standardization, and terminology normalization |
| Claims Preprocessing | Service categories, encounters, member months, claims enrollment, and provider attribution |
| Core Data Model | Common claims, clinical, cost, utilization, and medication outputs |
| Data Quality | Opt-in Structural and Logical Input Data Quality plus Output Data Quality rollups |
| Metadata and parity | Package metadata and an opt-in metric producer for release comparison |
| Data Assets | Version-aligned terminology, value sets, provider data, and synthetic data |

## Tuva 1.0 Package Ecosystem

Tuva 1.0 keeps the common transformation path in Core and distributes optional
marts and extensions as independently installable dbt packages. Installing a
standalone package enables that package; there is no umbrella package-enable
variable.

| Package | Scope |
| --- | --- |
| [AHRQ Quality Indicators](https://github.com/tuva-health/ahrq_quality_indicators) | AHRQ quality indicators and PQIs |
| [CCSR](https://github.com/tuva-health/ccsr) | Diagnosis and procedure CCSR groupers |
| [CMS Chronic Conditions](https://github.com/tuva-health/cms_chronic_conditions) | CMS-defined chronic conditions |
| [CMS HCC](https://github.com/tuva-health/cms_hcc) | CMS HCC scoring, recapture, and suspecting |
| [FHIR Preprocessing](https://github.com/tuva-health/fhir_preprocessing) | FHIR preprocessing extension |
| [NYU ED Classification](https://github.com/tuva-health/nyu_ed_classification) | Emergency-department classification |
| [Quality Measures](https://github.com/tuva-health/quality_measures) | Quality measures and retained readmissions scope |
| [Semantic Layer](https://github.com/tuva-health/semantic-layer) | Dimensions and facts over Core and selected packages |

Each package owns its models, tests, data assets, documentation,
compatibility, and release lifecycle.

## Install Tuva Core

Add a published version to the parent project's `packages.yml`:

```yaml
packages:
  - package: tuva-health/the_tuva_project
    version: "<published-version>"
```

For development against an explicitly reviewed Git ref:

```yaml
packages:
  - git: "https://github.com/tuva-health/tuva-core.git"
    revision: "<immutable-tag-or-commit>"
```

Install dependencies with `dbt deps`. The parent project must expose the Tuva
Input Layer models and enable the domains it maps:

```yaml
flags:
  require_ref_searches_node_package_before_root: true

vars:
  claims_enabled: true
  clinical_enabled: false
  provider_attribution_enabled: false
  data_quality_enabled: false
  use_coderx_enterprise: false
```

Feature variables must be native, unquoted YAML booleans. Quoted values and
direct `env_var()` expressions are strings and are rejected. Environment-driven
workflows should generate typed YAML or JSON before invoking dbt.

CodeRx Open is the default medication terminology. Setting
`use_coderx_enterprise: true` switches every CodeRx consumer to user-managed
`packages`, `drugs`, and `classes` relations in the target database's `coderx`
schema.

Build the package with dbt's test-aware command:

```bash
dbt build --select package:the_tuva_project
```

See [Getting Started](https://www.thetuvaproject.com/getting-started) and the
[dbt Variables reference](https://www.thetuvaproject.com/dbt-variables) for
connector, Input Layer, warehouse, and configuration details.

## Important 1.0 Contracts

- Public fields ending in `_flag` are nullable binary integers: `1` means true,
  `0` means false, and null means unknown or not applicable. Categorical values
  use `_code` or `_status` instead.
- Core `location` and `practitioner` are source-native. Their public keys are
  `(location_id, data_source)` and `(practitioner_id, data_source)`.
- Extension columns flow only between the 14 same-named Input Layer and Core
  tables: appointment, condition, eligibility, encounter, immunization,
  lab_result, location, medical_claim, medication, observation, patient,
  pharmacy_claim, practitioner, and procedure. They do not flow into derived
  outputs such as cost, member month, person ID crosswalk, or utilization.
- Open eligibility spans keep a null end date rather than being capped at the
  current run date.

The complete breaking-change catalog and upgrade guidance live in the
[Tuva documentation](https://www.thetuvaproject.com/).

## Local Development and Testing

Use `integration_tests` as the local parent dbt project. It imports this
checkout, maps versioned synthetic data into the Input Layer, and exercises
Tuva Core without installing the standalone packages.

Configure a supported adapter in `~/.dbt/profiles.yml`, then run from the
repository root:

```bash
scripts/dbt-local deps
scripts/dbt-local build --full-refresh \
  --select package:integration_tests package:the_tuva_project
```

For local DuckDB development, use one thread while loading data assets. See
[integration_tests/README.md](integration_tests/README.md) for the full local
profile example, variables, and CI contract.

Tuva Core uses dbt-native tests:

- YAML unit tests live next to the models they protect.
- Generic data tests live in model YAML, and singular data tests live under
  `tests/`.
- Opt-in parity models under `models/parity` produce cross-version release
  metrics.

Use `dbt build` for normal validation because `dbt run` does not execute unit
or data tests.

## Data Assets

Every asset-bearing 1.0 package resolves one complete snapshot from its
installed package version. Core assets use this layout:

```text
tuva-core/<package-version>/
├── terminology/
├── provider-data/
├── synthetic-data/
│   ├── small/
│   └── large/
└── value-sets/
```

Checked-in seed CSVs are header-only loader contracts. `data_assets.yml`
declares the exact Core payload inventory; users do not configure independent
terminology, value-set, provider-data, or synthetic-data versions.

Future-version payloads can be prepared before a version change reaches
`main`, but that payload-only prefix is a release candidate and must not contain
`_release.json`. After merge, the publisher writes the receipt only when the
complete snapshot verifies. The receipt binds the snapshot to the exact package
commit and must match across S3, GCS, and Azure before the package tag can be
created. dbt loaders do not read the receipt at runtime.

Redshift loading uses `IAM_ROLE default`; configure the cluster or serverless
namespace with a default IAM role that can read the selected data-asset bucket.
Released gzip objects carry `Content-Encoding: gzip` for compatible
object-store loading.

Release, publication, synthetic-data generation, and repository-maintenance
tooling lives in the separate `tuva-maintenance` repository. The Core-specific
`scripts/dbt-local` helper remains here because it runs this checkout through
the local integration project.

## Related Repositories

- [Documentation](https://www.thetuvaproject.com/)
- [DAG Viewer](https://github.com/tuva-health/dag-viewer)
- Tuva Maintenance (`tuva-health/tuva-maintenance`)

During local development, the docs site and DAG Viewer read this checkout
through `TUVA_CORE_PATH`.

## Contributing

- Report reproducible Core issues in
  [GitHub Issues](https://github.com/tuva-health/tuva-core/issues).
- Keep changes portable across every supported warehouse.
- Use the integration project and `dbt build` before opening a pull request.
- Coding agents must read [AGENTS.md](AGENTS.md) before changing the repository.

## License

Tuva Core is released under the [Apache 2.0 License](LICENSE).
