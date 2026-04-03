---
id: dbt-variables
title: "dbt Variables"
---

Set Tuva vars under the `vars:` key in your `dbt_project.yml`. Use selectors to run individual marts. The vars below control broad data domains, shared runtime behavior, and the synthetic bootstrap flow used by `integration_tests`.

```yaml
vars:
  claims_enabled: true
  clinical_enabled: true
  provider_attribution_enabled: true
  cms_hcc_payment_year: 2024
  quality_measures_period_end: "2024-12-31"
```

## Broad Enablement Vars

These vars are the main switches for the package. In the root package they default to `false` when omitted. In `integration_tests`, the same vars default to `true`.

| Variable | Root Default | `integration_tests` Default | Description |
|-----------|--------------|-----------------------------|-------------|
| `claims_enabled` | `false` | `true` | Enables claims-based models. |
| `clinical_enabled` | `false` | `true` | Enables clinical-based models. |
| `provider_attribution_enabled` | `false` | `true` | Enables provider attribution models. Claims input must also be enabled. |
| `semantic_layer_enabled` | `false` | `true` | Enables semantic-layer models. Claims-dependent semantic models also require `claims_enabled`. |
| `fhir_preprocessing_enabled` | `false` | `false` | Enables FHIR preprocessing models. |

## Shared Runtime Vars

These vars affect runtime behavior across the package.

| Variable | Default | Description |
|-----------|---------|-------------|
| `tuva_last_run` | Current UTC timestamp | Populates the `tuva_last_run` column in output models. |
| `tuva_schema_prefix` | unset | Prefixes output schemas, for example `myprefix_core`. |
| `cms_hcc_payment_year` | Current year | CMS-HCC payment year used for risk scoring. |
| `quality_measures_period_end` | Current year-end | Optional reporting-period end date for quality measures. |
| `record_type` | `"ip"` | CCSR record type: `"ip"` for inpatient or `"op"` for outpatient. |
| `dxccsr_version` | `"2023.1"` | CCSR diagnosis mapping version. |
| `prccsr_version` | `"2023.1"` | CCSR procedure mapping version. |
| `provider_attribution_as_of_date` | unset | Optional `YYYY-MM-DD` override for provider attribution current-state calculations. |
| `brand_generic_enabled` | inherits from `claims_enabled` | Optional pharmacy override for the brand/generic analysis models. |

## Seed And Feature Vars

These vars control shared seed loading and optional feature behavior.

| Variable | Default | Description |
|-----------|---------|-------------|
| `custom_bucket_name` | `"tuva-public-resources"` | Default bucket for versioned Tuva seed artifacts. |
| `tuva_seed_version` | `"1.0.0"` | Default versioned seed folder used when no per-database override is provided. Leading `v` is optional. |
| `tuva_seed_versions` | `{concept_library: "1.0.1", reference_data: "1.0.0", terminology: "1.0.0", value_sets: "1.0.0", provider_data: "1.0.0", synthetic_data: "1.0.0"}` | Optional per-database version overrides keyed by `concept_library`, `reference_data`, `terminology`, `value_sets`, `provider_data`, or `synthetic_data`. |
| `tuva_seed_buckets` | `{}` | Optional per-database bucket overrides for `concept_library`, `reference_data`, `terminology`, `value_sets`, `provider_data`, or `synthetic_data`. |
| `enable_input_layer_testing` | `true` | Runs DQI checks on the input layer. |
| `enable_legacy_data_quality` | `false` | Builds the legacy pre-DQI data-quality models. |
| `enable_normalize_engine` | `false` | Set to `unmapped` to surface unmapped code models, or `true` to also use custom mappings. |

## `integration_tests` Synthetic Bootstrap Vars

The `integration_tests` project is synthetic-only. `dbt seed` and `dbt build` load synthetic input data into `raw_data` from versioned S3 artifacts. `dbt run` assumes those `raw_data` tables already exist.

| Variable | Default | Description |
|-----------|---------|-------------|
| `synthetic_data_size` | `small` | Selects the `small` or `large` synthetic input payload. |
| `claims_enabled` | `true` | Enabled by default in `integration_tests`. |
| `clinical_enabled` | `true` | Enabled by default in `integration_tests`. |
| `provider_attribution_enabled` | `true` | Enabled by default in `integration_tests`. |
| `semantic_layer_enabled` | `true` | Enabled by default in `integration_tests`. |

## Example Root Package Config

Use broad domain vars plus any shared runtime overrides you need.

```yaml
vars:
  claims_enabled: true
  provider_attribution_enabled: true
  cms_hcc_payment_year: 2024
  quality_measures_period_end: "2024-12-31"
  tuva_seed_version: "1.0.0"
```

## Example `integration_tests` Config

Use the integration project to bootstrap a local DuckDB build with versioned synthetic data.

```yaml
vars:
  claims_enabled: true
  clinical_enabled: true
  provider_attribution_enabled: true
  semantic_layer_enabled: true
  synthetic_data_size: small
```

For provider attribution date overrides, see [Provider Attribution](./data-marts/tuva-provider-attribution.md). For the current local runbook, see [Getting Started](./getting-started.md).
