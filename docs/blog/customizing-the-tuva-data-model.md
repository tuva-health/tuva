---
title: Customizing the Tuva Data Model
description: How Tuva v0.17.0 preserves custom extension columns across the core data model with a configurable prefix-based pass-through pattern.
image: /img/tuva-extension-column-passthrough-diagram.png
tags: [tuva, dbt, healthcare-analytics]
authors:
  - name: Rabee Zyoud
    title: Healthcare Data & Analytics Consultant
    url: https://www.linkedin.com/in/zyoud/
date: 2026-02-12
toc_min_heading_level: 2
toc_max_heading_level: 2
---

_Rabee Zyoud is the founder of [SnowQuery](https://snowquery.com/), a healthcare data engineering and architecture consultancy. He's implemented Tuva for nearly 10 healthcare and life sciences organizations. For questions, implementation assistance, or consulting inquiries, contact hello@snowquery.com._

## Intro

Across most Tuva implementations I've worked on, teams need to carry organization-specific fields through the model. Common examples include care navigation ownership, payer-specific identifiers, and authorization metadata. Before v0.17.0, these fields often get dropped in core staging models because many models use explicit column lists.

With v0.17.0, Tuva introduces native extension-column pass-through support. Using a standard prefix convention (default: `x_`) and a reusable macro, host projects can add custom columns once and keep them available throughout Tuva core outputs.

<!--truncate-->

## The Problem

Healthcare organizations frequently need custom columns alongside Tuva's standardized models:

| Model           | Example Extension Columns                 |
| --------------- | ----------------------------------------- |
| `patient`       | `x_care_navigator`, `x_salesforce_id`     |
| `eligibility`   | `x_member_tier`                           |
| `medical_claim` | `x_authorization_number`, `x_referral_id` |
| `encounter`     | `x_department_name`                       |

Without pass-through support, those fields are preserved in `input_layer` but dropped in downstream core models that use explicit selections. Teams then have to maintain extra joins and rematerialize data downstream.

Typical impact:

- More custom SQL in host projects
- More tables/views to manage
- Higher maintenance and compute costs
- Slower downstream pipelines

In this post, I walk through:

- The pass-through pattern introduced in v0.17.0
- A before/after model example
- The `select_extension_columns` macro
- A practical adoption checklist

## What v0.17.0 Introduces

Tuva now supports extension-column pass-through driven by two variables:

```yaml
vars:
  passthrough:
    prefix: 'x_'    # Prefix that marks extension columns
    strip: false    # If true, remove prefix in final output aliases
```

| Variable | Default | Description |
|----------|---------|-------------|
| `passthrough.prefix` | `'x_'` | Prefix used to identify extension columns |
| `passthrough.strip` | `false` | Whether to strip prefix in final core model output |

### Data Flow

1. Host models publish standard Tuva columns plus extension columns (for example, `x_*`).
2. `input_layer__*` models still use `SELECT *`, so all fields are preserved.
3. Core staging/final models call `select_extension_columns(...)` to append extension columns dynamically.

![Extension Column Pass-Through Architecture](/img/tuva-extension-column-passthrough-diagram.png)

## How the Pattern Works

Core models are organized into column groups so extension behavior is explicit and reusable:

```sql
{%- set tuva_core_columns -%}
    -- Tuva standard columns
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__patient'), strip_prefix=false) }}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , data_source
    , tuva_last_run
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from ...
```

This keeps model SQL readable and ensures extension columns are not lost.

## Before and After Example

### Before (`core__stg_clinical_patient.sql`)

Extension columns from `input_layer__patient` are not selected.

```sql
select
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(first_name as {{ dbt.type_string() }}) as first_name
    -- explicit Tuva column list only
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , tuva_last_run_datetime as tuva_last_run
from {{ ref('input_layer__patient') }}
cross join tuva_last_run
```

### After (`core__stg_clinical_patient.sql`)

Extension columns are appended via macro.

```sql
{%- set tuva_core_columns -%}
    cast(person_id as {{ dbt.type_string() }}) as person_id,
    cast(first_name as {{ dbt.type_string() }}) as first_name,
    cast(data_source as {{ dbt.type_string() }}) as data_source
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__patient'), strip_prefix=false) }}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , tuva_last_run_datetime as tuva_last_run
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('input_layer__patient') }}
cross join tuva_last_run
```

This change in model structure is the key: standard columns stay explicit, while custom extension fields are appended dynamically.

## Host Project Usage Example

A host `patient` model can publish extension fields using the configured prefix:

```sql
select
      person_id
    , patient_id
    , first_name
    , last_name
    , birth_date
    , sex
    , race
    , care_navigator as x_care_navigator
    , salesforce_id as x_salesforce_id
    , risk_score as x_risk_score
    , primary_language as x_primary_language
    , data_source
from {{ source('source_input', 'patient') }}
```

In `core__patient`, you get:

- All standard Tuva columns
- `x_care_navigator` (or `care_navigator` when `passthrough.strip: true`)
- `x_salesforce_id` (or `salesforce_id` when `passthrough.strip: true`)
- `x_risk_score` (or `risk_score` when `passthrough.strip: true`)
- `x_primary_language` (or `primary_language` when `passthrough.strip: true`)

## The Macro

**Location:** `macros/core/select_extension_columns.sql`

Purpose:

- Detect columns by prefix from a relation using `adapter.get_columns_in_relation`
- Optionally qualify with alias
- Optionally strip prefix in output aliasing
- Return SQL-ready select expressions with leading commas

Core matching logic:

```sql
{%- for col in source_columns -%}
    {%- if col.name.lower().startswith(effective_prefix.lower()) -%}
        {%- set stripped_name = col.name[effective_prefix | length:] -%}
        {%- if effective_strip_prefix -%}
            {%- set col_expr = alias_prefix ~ col.name ~ ' as ' ~ stripped_name -%}
        {%- else -%}
            {%- set col_expr = alias_prefix ~ col.name -%}
        {%- endif -%}
        {%- do extension_columns.append(col_expr) -%}
    {%- endif -%}
{%- endfor -%}
```

<details>
<summary>Full macro code</summary>

```sql
{% macro select_extension_columns(relation, alias=none, prefix=none, strip_prefix=none) %}
    {%- if not execute -%}
        {{ return('') }}
    {%- endif -%}

    {%- set passthrough_config = var('passthrough', {}) -%}
    {%- set effective_prefix = prefix if prefix is not none else passthrough_config.get('prefix', 'x_') -%}
    {%- set effective_strip_prefix = strip_prefix if strip_prefix is not none else passthrough_config.get('strip', false) -%}

    {%- set source_columns = adapter.get_columns_in_relation(relation) -%}
    {%- if source_columns | length == 0 -%}
        {{ return('') }}
    {%- endif -%}

    {%- set alias_prefix = alias ~ '.' if alias else '' -%}
    {%- set extension_columns = [] -%}

    {%- for col in source_columns -%}
        {%- if col.name.lower().startswith(effective_prefix.lower()) -%}
            {%- set stripped_name = col.name[effective_prefix | length:] -%}
            {%- if effective_strip_prefix -%}
                {%- set col_expr = alias_prefix ~ col.name ~ ' as ' ~ stripped_name -%}
            {%- else -%}
                {%- set col_expr = alias_prefix ~ col.name -%}
            {%- endif -%}
            {%- do extension_columns.append(col_expr) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if extension_columns | length > 0 -%}
        {%- for col_expr in extension_columns %}
    , {{ col_expr }}
        {%- endfor -%}
    {%- endif -%}
{% endmacro %}
```
</details>

### Macro Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `relation` | required | Relation to inspect for extension columns |
| `alias` | `none` | Optional table alias for column references |
| `prefix` | `var('passthrough').get('prefix', 'x_')` | Prefix that identifies extension columns |
| `strip_prefix` | `var('passthrough').get('strip', false)` | Remove prefix in output alias |

### Macro Usage Examples

Keep prefix:

```sql
{{ select_extension_columns(ref('input_layer__patient'), strip_prefix=false) }}
```

With alias:

```sql
{{ select_extension_columns(ref('input_layer__medical_claim'), alias='claim', strip_prefix=false) }}
```

Use global strip configuration:

```sql
{{ select_extension_columns(ref('input_layer__patient')) }}
```
## Other Details

### Naming Convention

| Prefix | Meaning |
| ------ | ------- |
| `x_`   | Extension column passed through Tuva core models |

Why this default works well:

- Short and easy to scan
- Clear distinction from standard Tuva columns
- Low risk of naming collision
- Optional removal in outputs via `passthrough.strip`

### Implementation Scope in Tuva

The v0.17.0 release applies this pattern across macros and core models, including:

| Area | Example Files |
| ---- | ------------- |
| Macros | `macros/core/select_extension_columns.sql`, `macros/core/smart_union.sql` |
| Staging models | `core__stg_clinical_patient.sql`, `core__stg_claims_medical_claim.sql`, `core__stg_clinical_eligibility.sql` |
| Final models | `core__condition.sql`, `core__procedure.sql`, `core__medication.sql`, `core__lab_result.sql`, `core__observation.sql` |

### Adoption Checklist

1. Add extension fields in host input models with the configured prefix (default `x_`).
2. Set `vars.passthrough.prefix` and optional `vars.passthrough.strip` in `dbt_project.yml`.
3. Confirm core models use `select_extension_columns(...)` where explicit column lists are present.
4. Run model builds and validate expected extension columns in core outputs.

## Closing

This pattern lets teams preserve organization-specific fields without forking Tuva or maintaining heavy downstream rejoin logic. The result is cleaner host projects, less duplicate SQL, and more reusable Tuva outputs for marts and analytics.
