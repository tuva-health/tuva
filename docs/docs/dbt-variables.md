---
id: dbt-variables
title: "dbt Variables"
---

The Tuva Project includes a number of Data Marts and some Data Marts have optional parameters.  We use dbt variables to control the behavior of these parameters.  This section describes these variables and how to use them.

```yaml
vars:
  cms_hcc_payment_year: 2024
  quality_measurement_year: 2024
  period_end: '2024-12-31'
```

## Year-Specific Variables in `dbt_project.yml`

Tuva’s Input Layer and Data Marts rely on year-specific reference data for things like:

- Risk adjustment models (e.g., CMS HCCs)
- Quality measure specifications
- Benchmark values
- Period-based data filtering

These parameters can be set in the `vars:` section of your `dbt_project.yml` to customize or lock behavior.

```yaml
vars:
  claims_enabled: true

  # Optional year-specific parameters
  cms_hcc_payment_year: 2024
  quality_measurement_year: 2024
  period_end: '2024-12-31'
```

## Parameter Reference

| Parameter                  | Description                                                                 | Example         |
|---------------------------|-----------------------------------------------------------------------------|-----------------|
| `cms_hcc_payment_year`     | Specifies the CMS HCC model year (used for risk scoring).                   | `2023`          |
| `quality_measurement_year`| Specifies the measurement year for quality metrics (e.g., Stars, HEDIS).     | `2024`          |
| `period_end`              | Optional date filter to exclude data after a certain date.                  | `'2022-12-31'`  |

## When to Use These

You only need to override the defaults if:
- You're analyzing **past years** (e.g., historical claims)
- You want to **lock behavior** for reproducibility or backtesting
- You're setting up a **static reporting period**

## Example Use Case

Analyzing 2022 claims using the 2023 HCC model:

```yaml
vars:
  claims_enabled: true
  cms_hcc_payment_year: 2023
  period_end: '2022-12-31'
```

This ensures that:
- Only claims through 2022 are included
- Risk scores use the 2023 model coefficients
