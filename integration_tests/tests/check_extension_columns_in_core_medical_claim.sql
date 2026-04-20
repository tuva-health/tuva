-- Regression test: extension columns from input_layer__medical_claim must be
-- present and correctly populated in core.medical_claim.
--
-- Returns rows (failures) if:
--   1. x_temp_claim_id does not equal claim_id (value corruption check).
--   2. x_temp_payer does not equal payer (value corruption check).
--   3. All x_ column values are null across every medical claim row when the
--      table has data (population check).
--
-- The integration-test setup defines these extensions in
-- integration_tests/models/medical_claim.sql:
--     claim_id as x_temp_claim_id
--     payer    as x_temp_payer
--
-- If no x_ columns are configured in the input layer the test passes vacuously.

-- depends_on: {{ ref('core__medical_claim') }}

{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool,
     tags = ['extension_columns'],
     severity = 'error'
   )
}}

{%- set source_relation = ref('input_layer__medical_claim') -%}
{%- set extension_cols = [] -%}
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

{%- if 'x_temp_claim_id' in extension_cols %}

select
    claim_id
    , claim_line_number
    , 'x_temp_claim_id does not match claim_id' as failure_reason
from {{ ref('core__medical_claim') }}
where cast(x_temp_claim_id as varchar) <> cast(claim_id as varchar)
   or (x_temp_claim_id is null     and claim_id is not null)
   or (x_temp_claim_id is not null and claim_id is null)

{%- else %}

select
    null as claim_id
    , null as claim_line_number
    , null as failure_reason
where false

{%- endif %}

union all

{%- if 'x_temp_payer' in extension_cols %}

select
    claim_id
    , claim_line_number
    , 'x_temp_payer does not match payer' as failure_reason
from {{ ref('core__medical_claim') }}
where cast(x_temp_payer as varchar) <> cast(payer as varchar)
   or (x_temp_payer is null     and payer is not null)
   or (x_temp_payer is not null and payer is null)

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
    , 'all x_ column values are null across every medical claim row' as failure_reason
from (
    select
        {% for col in extension_cols %}
        count({{ col }}) as {{ col }}_non_null
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('core__medical_claim') }}
) counts
where
    {% for col in extension_cols %}
    {{ col }}_non_null = 0
    {%- if not loop.last %} and {% endif %}
    {% endfor %}
    and (select count(*) from {{ ref('core__medical_claim') }}) > 0

{%- endif -%}
