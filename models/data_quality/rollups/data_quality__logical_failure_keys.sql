{{ config(
     enabled = (var('enable_data_quality', false) | as_bool)
       and (var('enable_data_quality_failure_keys', false) | as_bool)
       and ((var('claims_enabled', false) | as_bool) or (var('clinical_enabled', false) | as_bool)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'logical_failure_keys',
     tags = ['data_quality', 'dq', 'dq1', 'dq_rollup'],
     materialized = 'table'
   )
}}

{% set key_queries = [] %}
{% set string_type = dbt.type_string() %}

{% for definition in dq_enabled_logical_test_manifest() %}
    {% set key_value_parts = [] %}
    {% for key_column in definition['key_columns'] %}
        {% do key_value_parts.append("'" ~ key_column ~ "='") %}
        {% do key_value_parts.append("coalesce(cast(flags." ~ quote_column(key_column) ~ " as " ~ string_type ~ "), '')") %}
        {% if not loop.last %}
            {% do key_value_parts.append("'|'") %}
        {% endif %}
    {% endfor %}

    {% set query %}
        select
              cast(flags.data_source as {{ string_type }}) as data_source
            , {{ dq_string_literal_sql(definition['input_table_name']) }} as input_table_name
            , {{ dq_string_literal_sql(definition['test_name']) }} as test_name
            , {{ dq_string_literal_sql(definition['display_name']) }} as display_name
            , {{ dq_string_literal_sql(definition['description']) }} as description
            , {{ dq_string_literal_sql(definition['test_type']) }} as test_type
            , cast({{ definition['severity'] }} as {{ dbt.type_int() }}) as severity
            , {{ dq_string_literal_sql(definition['key_columns'] | join(',')) }} as key_columns
            , {{ concat_custom(key_value_parts) }} as key_values
        from {{ ref(definition['source_model_name']) }} as flags
        where flags.{{ quote_column(definition['flag_column_name']) }} = 1
    {% endset %}
    {% do key_queries.append(query) %}
{% endfor %}

{% if key_queries | length > 0 %}
    select *
    from (
        {{ key_queries | join('\nunion all\n') }}
    ) as logical_failure_keys
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as input_table_name
        , cast(null as {{ dbt.type_string() }}) as test_name
        , cast(null as {{ dbt.type_string() }}) as display_name
        , cast(null as {{ dbt.type_string() }}) as description
        , cast(null as {{ dbt.type_string() }}) as test_type
        , cast(null as {{ dbt.type_int() }}) as severity
        , cast(null as {{ dbt.type_string() }}) as key_columns
        , cast(null as {{ dbt.type_string() }}) as key_values
    {{ dq_empty_result_guard_sql() }}
{% endif %}
