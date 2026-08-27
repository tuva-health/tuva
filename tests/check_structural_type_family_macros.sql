{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     tags = ['data_quality', 'dq_structural']
   )
}}

{% set type_family_cases = [
    ('varchar_alias', dq_type_family('VARCHAR'), 'string'),
    ('bigint_alias', dq_type_family('BIGINT'), 'integer'),
    ('boolean_alias', dq_type_family('BOOL'), 'boolean'),
    ('date_alias', dq_type_family('DATE'), 'date'),
    ('timestamp_alias', dq_type_family('TIMESTAMP_NTZ'), 'timestamp'),
    ('scaled_numeric', dq_type_family('DECIMAL(18,2)'), 'numeric'),
    ('zero_scale_number', dq_type_family('NUMBER(38,0)'), 'integer'),
    ('bare_number', dq_type_family('NUMBER'), 'numeric'),
    ('bigquery_bytes', bigquery__dq_type_family('BYTES'), 'binary')
] %}

with type_family_results as (
    {% for case in type_family_cases %}
    select
          '{{ case[0] }}' as test_case
        , '{{ case[1] }}' as actual_type_family
        , '{{ case[2] }}' as expected_type_family
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select *
from type_family_results
where actual_type_family <> expected_type_family
