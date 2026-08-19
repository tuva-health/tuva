{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool)
       and ((var('claims_enabled', false) | as_bool) or (var('clinical_enabled', false) | as_bool)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'logical_test_input_columns',
     tags = ['data_quality', 'dq', 'dq1', 'dq_rollup'],
     materialized = 'table'
   )
}}

{% set column_queries = [] %}

{% for definition in dq_enabled_logical_test_manifest() %}
    {% for input_column_name in dq_logical_test_input_columns(definition['test_name']) %}
        {% set query %}
            select
                  {{ dq_string_literal_sql(definition['test_name']) }} as test_name
                , {{ dq_string_literal_sql(definition['input_table_name']) }} as input_table_name
                , {{ dq_string_literal_sql(input_column_name) }} as input_column_name
        {% endset %}
        {% do column_queries.append(query) %}
    {% endfor %}
{% endfor %}

{% if column_queries | length > 0 %}
    select *
    from (
        {{ column_queries | join('\nunion all\n') }}
    ) as logical_test_input_columns
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as test_name
        , cast(null as {{ dbt.type_string() }}) as input_table_name
        , cast(null as {{ dbt.type_string() }}) as input_column_name
    {{ dq_empty_result_guard_sql() }}
{% endif %}
