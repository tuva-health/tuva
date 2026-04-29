-- Regression test: extension columns from input_layer__pharmacy_claim must be
-- present and correctly populated in core.pharmacy_claim.
--
-- Returns rows (failures) if:
--   1. x_temp_ndc_code does not equal ndc_code (value corruption check).
--   2. All x_ column values are null across every pharmacy claim row when the
--      table has data (population check).
--
-- The integration-test setup defines:
--     ndc_code as x_temp_ndc_code
-- in integration_tests/models/pharmacy_claim.sql.
--
-- If no x_ columns are configured in the input layer the test passes vacuously.

-- depends_on: {{ ref('core__pharmacy_claim') }}

{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool,
     tags = ['extension_columns'],
     severity = 'error'
   )
}}

{%- set source_relation = ref('input_layer__pharmacy_claim') -%}
{%- set extension_cols = [] -%}
{%- set string_type = dbt.type_string() -%}
{%- for col in adapter.get_columns_in_relation(source_relation) -%}
    {%- if col.name.lower().startswith('x_') -%}
        {%- do extension_cols.append(col.name.lower()) -%}
    {%- endif -%}
{%- endfor -%}

{%- if extension_cols | length == 0 -%}

select
    null as claim_id
    , null as claim_line_number
    , null as failure_reason
where false

{%- else -%}

{%- if 'x_temp_ndc_code' in extension_cols %}

select
    claim_id
    , claim_line_number
    , 'x_temp_ndc_code does not match ndc_code' as failure_reason
from {{ ref('core__pharmacy_claim') }}
where cast(x_temp_ndc_code as {{ string_type }}) <> cast(ndc_code as {{ string_type }})
   or (x_temp_ndc_code is null     and ndc_code is not null)
   or (x_temp_ndc_code is not null and ndc_code is null)

{%- else %}

select
    null as claim_id
    , null as claim_line_number
    , null as failure_reason
where false

{%- endif %}

union all

select
    null as claim_id
    , null as claim_line_number
    , 'all x_ column values are null across every pharmacy claim row' as failure_reason
from (
    select
        {% for col in extension_cols %}
        count({{ col }}) as {{ col }}_non_null
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('core__pharmacy_claim') }}
) counts
where
    {% for col in extension_cols %}
    {{ col }}_non_null = 0
    {%- if not loop.last %} and {% endif %}
    {% endfor %}
    and (select count(*) from {{ ref('core__pharmacy_claim') }}) > 0

{%- endif -%}
