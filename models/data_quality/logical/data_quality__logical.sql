{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('claims_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'logical',
     tags = ['data_quality', 'dq', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

{% if var('claims_enabled', false) | as_bool %}
    {% set logical_queries = [] %}

    {% for definition in dq_logical_test_manifest() %}
        {% do logical_queries.append(dq_logical_sum_flag_query_sql(definition)) %}
    {% endfor %}

    {% if logical_queries | length > 0 %}
        select *
        from (
            {{ logical_queries | join('\nunion all\n') }}
        ) as logical_results
        order by 1, 2, 3
    {% else %}
        select
              cast(null as {{ dbt.type_string() }}) as data_source
            , cast(null as {{ dbt.type_string() }}) as {{ adapter.quote('table') }}
            , cast(null as {{ dbt.type_string() }}) as test_name
            , cast(null as {{ dbt.type_int() }}) as test_result
        {{ dq_empty_result_guard_sql() }}
    {% endif %}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as {{ adapter.quote('table') }}
        , cast(null as {{ dbt.type_string() }}) as test_name
        , cast(null as {{ dbt.type_int() }}) as test_result
    {{ dq_empty_result_guard_sql() }}
{% endif %}
