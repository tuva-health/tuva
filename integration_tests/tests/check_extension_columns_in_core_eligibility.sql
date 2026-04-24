-- Regression test: extension columns from input_layer__eligibility must be
-- present and correctly populated in core.eligibility.
--
-- Returns rows (failures) if:
--   1. x_temp_person_id does not equal person_id (value corruption check).
--   2. All x_ column values are null across every eligibility row when the
--      table has data (population check).
--
-- If no x_ columns are configured in the input layer the test passes vacuously.

-- depends_on: {{ ref('core__eligibility') }}

{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool,
     tags = ['extension_columns'],
     severity = 'error'
   )
}}

{%- set source_relation = ref('input_layer__eligibility') -%}
{%- set extension_cols = [] -%}
{%- for col in adapter.get_columns_in_relation(source_relation) -%}
    {%- if col.name.lower().startswith('x_') -%}
        {%- do extension_cols.append(col.name.lower()) -%}
    {%- endif -%}
{%- endfor -%}

{%- if extension_cols | length == 0 -%}

select
    null as eligibility_id
    , null as failure_reason
where false

{%- else -%}

{%- if 'x_temp_person_id' in extension_cols %}

select
    eligibility_id
    , 'x_temp_person_id does not match person_id' as failure_reason
from {{ ref('core__eligibility') }}
where cast(x_temp_person_id as {{ dbt.type_string() }}) <> cast(person_id as {{ dbt.type_string() }})
   or (x_temp_person_id is null     and person_id is not null)
   or (x_temp_person_id is not null and person_id is null)

{%- else %}

select
    null as eligibility_id
    , null as failure_reason
where false

{%- endif %}

union all

select
    null as eligibility_id
    , 'all x_ column values are null across every eligibility row' as failure_reason
from (
    select
        {% for col in extension_cols %}
        count({{ col }}) as {{ col }}_non_null
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('core__eligibility') }}
) counts
where
    {% for col in extension_cols %}
    {{ col }}_non_null = 0
    {%- if not loop.last %} and {% endif %}
    {% endfor %}
    and (select count(*) from {{ ref('core__eligibility') }}) > 0

{%- endif -%}
