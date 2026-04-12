-- Regression test: extension columns from input_layer__eligibility must be
-- present and correctly populated in core.member_months.
--
-- Returns rows (failures) if any of the following are true:
--   1. An x_ column that exists in input_layer__eligibility is absent from
--      core.member_months (SQL fails to compile → caught as an error).
--   2. x_temp_person_id does not equal person_id for any row, which would
--      indicate a value was corrupted or the wrong source column was wired up.
--      (x_temp_person_id is defined as `person_id as x_temp_person_id` in
--       integration_tests/models/eligibility.sql.)
--
-- If no x_ columns are configured in the input layer the test passes vacuously.

-- depends_on: {{ ref('core__member_months') }}

{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', false))) | as_bool,
     tags = ['extension_columns'],
     severity = 'error'
   )
}}

{%- set source_relation = ref('input_layer__eligibility') -%}
{%- set extension_cols = [] -%}
{%- for col in adapter.get_columns_in_relation(source_relation) -%}
    {%- if col.name.lower().startswith('x_') -%}
        {%- do extension_cols.append(col.name) -%}
    {%- endif -%}
{%- endfor -%}

{%- if extension_cols | length == 0 -%}

-- No x_ columns configured in input_layer__eligibility; test passes vacuously.
select 1 where false

{%- else -%}

-- 1. Verify x_temp_person_id = person_id (invariant for the integration-test
--    setup where eligibility.sql defines: person_id as x_temp_person_id).
{%- if 'x_temp_person_id' in extension_cols %}

select
    member_month_key
    , 'x_temp_person_id does not match person_id' as failure_reason
from {{ ref('core__member_months') }}
where cast(x_temp_person_id as varchar) <> cast(person_id as varchar)
   or (x_temp_person_id is null     and person_id is not null)
   or (x_temp_person_id is not null and person_id is null)

{%- else %}

-- x_temp_person_id not in extension columns; pass vacuously for this check.
select 1 where false

{%- endif %}

union all

-- 2. Verify that at least one row has a non-null value across any x_ column,
--    confirming the pipeline is not silently producing all-null extension data.
select
    null as member_month_key
    , 'all x_ column values are null across every member-month row' as failure_reason
from (
    select
        {% for col in extension_cols %}
        count({{ col }}) as {{ col }}_non_null
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('core__member_months') }}
) counts
where
    {% for col in extension_cols %}
    {{ col }}_non_null = 0
    {%- if not loop.last %} and {% endif %}
    {% endfor %}
    -- Only fail if the table actually has rows
    and (select count(*) from {{ ref('core__member_months') }}) > 0

{%- endif -%}
