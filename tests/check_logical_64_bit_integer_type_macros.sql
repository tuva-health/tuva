{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

{#
  These compile-time boundaries keep the semantic BIGINT contract portable.
  The adapter-specific implementations are invoked directly so every branch
  remains covered even when this test runs on only one warehouse.
#}

{% set capacity_cases = [
    ('dispatch_bigint', dq_type_has_64_bit_integer_capacity(dbt.type_bigint()), true),
    ('default_bigint', default__dq_type_has_64_bit_integer_capacity('BIGINT'), true),
    ('default_int8', default__dq_type_has_64_bit_integer_capacity('INT8'), true),
    ('default_int64', default__dq_type_has_64_bit_integer_capacity('INT64'), true),
    ('default_long', default__dq_type_has_64_bit_integer_capacity('LONG'), true),
    ('default_hugeint', default__dq_type_has_64_bit_integer_capacity('HUGEINT'), true),
    ('default_decimal_19_scale_0', default__dq_type_has_64_bit_integer_capacity('DECIMAL(19,0)'), true),
    ('default_numeric_38_scale_0_with_spaces', default__dq_type_has_64_bit_integer_capacity(' NUMERIC ( 38, 0 ) '), true),
    ('default_number_19_scale_0', default__dq_type_has_64_bit_integer_capacity('NUMBER(19,0)'), true),
    ('default_integer_fails_closed', default__dq_type_has_64_bit_integer_capacity('INTEGER'), false),
    ('default_decimal_precision_18', default__dq_type_has_64_bit_integer_capacity('DECIMAL(18,0)'), false),
    ('default_decimal_nonzero_scale', default__dq_type_has_64_bit_integer_capacity('DECIMAL(38,1)'), false),
    ('default_bare_number_fails_closed', default__dq_type_has_64_bit_integer_capacity('NUMBER'), false),
    ('default_missing_scale_fails_closed', default__dq_type_has_64_bit_integer_capacity('NUMBER(38)'), false),
    ('default_malformed_precision_fails_closed', default__dq_type_has_64_bit_integer_capacity('NUMBER(not_a_precision,0)'), false),
    ('default_trailing_content_fails_closed', default__dq_type_has_64_bit_integer_capacity('NUMBER(38,0)(extra)'), false),
    ('default_unknown_fails_closed', default__dq_type_has_64_bit_integer_capacity('MYSTERY_INTEGER'), false),
    ('default_none_fails_closed', default__dq_type_has_64_bit_integer_capacity(none), false),
    ('snowflake_integer_alias', snowflake__dq_type_has_64_bit_integer_capacity('INTEGER'), true),
    ('snowflake_smallint_alias', snowflake__dq_type_has_64_bit_integer_capacity('SMALLINT'), true),
    ('snowflake_bare_number', snowflake__dq_type_has_64_bit_integer_capacity('NUMBER'), true),
    ('snowflake_number_precision_18', snowflake__dq_type_has_64_bit_integer_capacity('NUMBER(18,0)'), false),
    ('snowflake_number_nonzero_scale', snowflake__dq_type_has_64_bit_integer_capacity('NUMBER(38,2)'), false),
    ('bigquery_int64', bigquery__dq_type_has_64_bit_integer_capacity('INT64'), true),
    ('bigquery_integer_alias', bigquery__dq_type_has_64_bit_integer_capacity('INTEGER'), true),
    ('bigquery_smallint_alias', bigquery__dq_type_has_64_bit_integer_capacity('SMALLINT'), true),
    ('bigquery_float64_fails_closed', bigquery__dq_type_has_64_bit_integer_capacity('FLOAT64'), false),
    ('bigquery_bare_numeric_fails_closed', bigquery__dq_type_has_64_bit_integer_capacity('NUMERIC'), false)
] %}

{% set capacity_failures = [] %}

{% for capacity_case in capacity_cases %}
    {% if capacity_case[1] != capacity_case[2] %}
        {% set failure_query %}
            select
                  {{ dq_string_literal_sql(capacity_case[0]) }} as test_case
                , cast({{ 1 if capacity_case[1] else 0 }} as {{ dbt.type_int() }}) as actual_result
                , cast({{ 1 if capacity_case[2] else 0 }} as {{ dbt.type_int() }}) as expected_result
        {% endset %}
        {% do capacity_failures.append(failure_query | trim) %}
    {% endif %}
{% endfor %}

{% if capacity_failures | length > 0 %}
    {{ capacity_failures | join('\nunion all\n') }}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as test_case
        , cast(null as {{ dbt.type_int() }}) as actual_result
        , cast(null as {{ dbt.type_int() }}) as expected_result
    {{ dq_empty_result_guard_sql() }}
{% endif %}
