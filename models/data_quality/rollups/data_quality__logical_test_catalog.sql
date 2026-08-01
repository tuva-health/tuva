{{ config(
     enabled = (var('enable_data_quality', false) | as_bool)
       and ((var('claims_enabled', false) | as_bool) or (var('clinical_enabled', false) | as_bool)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'logical_test_catalog',
     tags = ['data_quality', 'dq', 'dq1', 'dq_rollup'],
     materialized = 'table'
   )
}}

{% set catalog_queries = [] %}

{% for definition in dq_enabled_logical_test_manifest() %}
    {% set query %}
        select
              {{ dq_string_literal_sql(definition['test_name']) }} as test_name
            , {{ dq_string_literal_sql(definition['display_name']) }} as display_name
            , {{ dq_string_literal_sql(definition['description']) }} as description
            , {{ dq_string_literal_sql(definition['input_model_name']) }} as input_model_name
            , {{ dq_string_literal_sql(definition['input_table_name']) }} as input_table_name
            , {{ dq_string_literal_sql(definition['source_model_name']) }} as flag_model_name
            , {{ dq_string_literal_sql(definition['flag_table_name']) }} as flag_table_name
            , {{ dq_string_literal_sql(definition['flag_column_name']) }} as flag_column_name
            , {{ dq_string_literal_sql(definition['grain']) }} as grain
            , {{ dq_string_literal_sql(definition['key_columns'] | join(',')) }} as key_columns
            , {{ dq_string_literal_sql(definition['test_type']) }} as test_type
            , {{ dq_string_literal_sql(definition['test_type']) }} as check_category
            , cast({{ definition['severity'] }} as {{ dbt.type_int() }}) as severity
            , cast({{ definition['severity'] }} as {{ dbt.type_int() }}) as default_severity
            , {{ dq_string_literal_sql(definition['investigation_sql']) }} as investigation_sql
    {% endset %}
    {% do catalog_queries.append(query) %}
{% endfor %}

{% if catalog_queries | length > 0 %}
    select *
    from (
        {{ catalog_queries | join('\nunion all\n') }}
    ) as logical_test_catalog
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as test_name
        , cast(null as {{ dbt.type_string() }}) as display_name
        , cast(null as {{ dbt.type_string() }}) as description
        , cast(null as {{ dbt.type_string() }}) as input_model_name
        , cast(null as {{ dbt.type_string() }}) as input_table_name
        , cast(null as {{ dbt.type_string() }}) as flag_model_name
        , cast(null as {{ dbt.type_string() }}) as flag_table_name
        , cast(null as {{ dbt.type_string() }}) as flag_column_name
        , cast(null as {{ dbt.type_string() }}) as grain
        , cast(null as {{ dbt.type_string() }}) as key_columns
        , cast(null as {{ dbt.type_string() }}) as test_type
        , cast(null as {{ dbt.type_string() }}) as check_category
        , cast(null as {{ dbt.type_int() }}) as severity
        , cast(null as {{ dbt.type_int() }}) as default_severity
        , cast(null as {{ dbt.type_string() }}) as investigation_sql
    {{ dq_empty_result_guard_sql() }}
{% endif %}
