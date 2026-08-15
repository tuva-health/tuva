{{ config(
     enabled = ((var('enable_data_quality', false) | string | lower) == 'true')
       and (((var('claims_enabled', false) | string | lower) == 'true') or ((var('clinical_enabled', false) | string | lower) == 'true'))
   )
}}

{% set result_queries = [] %}

{% for definition in dq_enabled_logical_test_manifest() %}
    {% set flag_column = quote_column(definition['flag_column_name']) %}
    {% set query %}
        select
              cast(data_source as {{ dbt.type_string() }}) as data_source
            , {{ dq_string_literal_sql(definition['input_table_name']) }} as input_table_name
            , {{ dq_string_literal_sql(definition['test_name']) }} as test_name
            , {{ dq_string_literal_sql(definition['display_name']) }} as display_name
            , {{ dq_string_literal_sql(definition['description']) }} as description
            , {{ dq_string_literal_sql(definition['grain']) }} as grain
            , {{ dq_string_literal_sql(definition['flag_table_name']) }} as flag_table_name
            , {{ dq_string_literal_sql(definition['flag_column_name']) }} as flag_column_name
            , {{ dq_string_literal_sql(definition['test_type']) }} as test_type
            , {{ dq_string_literal_sql(definition['test_type']) }} as check_category
            , cast({{ definition['severity'] }} as {{ dbt.type_int() }}) as severity
            , cast(count(*) as {{ dbt.type_int() }}) as total_row_count
            , cast(sum(case when {{ flag_column }} in (0, 1) then 1 else 0 end) as {{ dbt.type_int() }}) as tested_count
            , cast(sum(cast(coalesce({{ flag_column }}, 0) as {{ dbt.type_int() }})) as {{ dbt.type_int() }}) as failed_count
            , cast(
                sum(case when {{ flag_column }} in (0, 1) then 1 else 0 end)
                - sum(cast(coalesce({{ flag_column }}, 0) as {{ dbt.type_int() }}))
              as {{ dbt.type_int() }}) as passed_count
            , cast(sum(case when {{ flag_column }} is null then 1 else 0 end) as {{ dbt.type_int() }}) as not_applicable_count
        from {{ ref(definition['source_model_name']) }}
        group by cast(data_source as {{ dbt.type_string() }})
    {% endset %}
    {% do result_queries.append(query) %}
{% endfor %}

{% if result_queries | length > 0 %}
    select *
    from (
        {{ result_queries | join('\nunion all\n') }}
    ) as logical_test_results
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as input_table_name
        , cast(null as {{ dbt.type_string() }}) as test_name
        , cast(null as {{ dbt.type_string() }}) as display_name
        , cast(null as {{ dbt.type_string() }}) as description
        , cast(null as {{ dbt.type_string() }}) as grain
        , cast(null as {{ dbt.type_string() }}) as flag_table_name
        , cast(null as {{ dbt.type_string() }}) as flag_column_name
        , cast(null as {{ dbt.type_string() }}) as test_type
        , cast(null as {{ dbt.type_string() }}) as check_category
        , cast(null as {{ dbt.type_int() }}) as severity
        , cast(null as {{ dbt.type_int() }}) as total_row_count
        , cast(null as {{ dbt.type_int() }}) as tested_count
        , cast(null as {{ dbt.type_int() }}) as failed_count
        , cast(null as {{ dbt.type_int() }}) as passed_count
        , cast(null as {{ dbt.type_int() }}) as not_applicable_count
    {{ dq_empty_result_guard_sql() }}
{% endif %}
