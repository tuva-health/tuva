{{ config(
     enabled = var('enable_data_quality', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_actual_columns',
     tags = ['data_quality', 'dq', 'dq1', 'dq_structural'],
     materialized = 'table'
   )
}}

{% set input_layer_model_names = dq_enabled_input_layer_model_names() %}

{% for model_name in input_layer_model_names %}
-- depends_on: {{ ref(model_name) }}
{% endfor %}

{% if execute %}
    {% set actual_queries = [] %}

    {% for model_node in dq_expected_input_layer_models() %}
        {% set table_name = model_node.name | replace('input_layer__', '') %}
        {% set relation = dq_actual_relation(model_node) %}

        {% if relation is none %}
            {% set query %}
                select
                      cast(null as {{ dbt.type_string() }}) as data_source
                    , '{{ dq_source_key_sentinel() }}' as data_source_key
                    , '{{ table_name }}' as table_name
                    , '{{ model_node.name }}' as model_name
                    , cast(null as {{ dbt.type_string() }}) as column_name
                    , cast(null as {{ dbt.type_string() }}) as actual_data_type
                    , cast(null as {{ dbt.type_string() }}) as actual_type_family
                    , 'no' as table_exists
                    , cast(null as {{ dbt.type_int() }}) as row_count
            {% endset %}

            {% do actual_queries.append(query) %}
        {% else %}
            {% set source_dimension_sql = dq_source_dimension_sql(relation) %}
            {% set source_count_sql = dq_source_row_count_sql(relation) %}
            {% set column_queries = [] %}

            {% for column in dq_actual_columns(relation) %}
                {% set column_query %}
                    select
                          '{{ column.name | lower | replace("'", "''") }}' as column_name
                        , '{{ column.dtype | replace("'", "''") }}' as actual_data_type
                        , '{{ dq_type_family(column.dtype) }}' as actual_type_family
                {% endset %}

                {% do column_queries.append(column_query) %}
            {% endfor %}

            {% set query %}
                select
                      sources.data_source
                    , sources.data_source_key
                    , '{{ table_name }}' as table_name
                    , '{{ model_node.name }}' as model_name
                    , actual_columns.column_name
                    , actual_columns.actual_data_type
                    , actual_columns.actual_type_family
                    , 'yes' as table_exists
                    , cast(coalesce(source_counts.row_count, 0) as {{ dbt.type_int() }}) as row_count
                from (
                    {{ source_dimension_sql }}
                ) as sources
                left join (
                    {{ source_count_sql }}
                ) as source_counts
                    on sources.data_source_key = source_counts.data_source_key
                cross join (
                    {{ column_queries | join('\nunion all\n') }}
                ) as actual_columns
            {% endset %}

            {% do actual_queries.append(query) %}
        {% endif %}
    {% endfor %}

    {% if actual_queries | length > 0 %}
        select *
        from (
            {{ actual_queries | join('\nunion all\n') }}
        ) as structural_actual_columns
    {% else %}
        select
              cast(null as {{ dbt.type_string() }}) as data_source
            , cast(null as {{ dbt.type_string() }}) as data_source_key
            , cast(null as {{ dbt.type_string() }}) as table_name
            , cast(null as {{ dbt.type_string() }}) as model_name
            , cast(null as {{ dbt.type_string() }}) as column_name
            , cast(null as {{ dbt.type_string() }}) as actual_data_type
            , cast(null as {{ dbt.type_string() }}) as actual_type_family
            , cast(null as {{ dbt.type_string() }}) as table_exists
            , cast(null as {{ dbt.type_int() }}) as row_count
        {{ dq_empty_result_guard_sql() }}
    {% endif %}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as data_source_key
        , cast(null as {{ dbt.type_string() }}) as table_name
        , cast(null as {{ dbt.type_string() }}) as model_name
        , cast(null as {{ dbt.type_string() }}) as column_name
        , cast(null as {{ dbt.type_string() }}) as actual_data_type
        , cast(null as {{ dbt.type_string() }}) as actual_type_family
        , cast(null as {{ dbt.type_string() }}) as table_exists
        , cast(null as {{ dbt.type_int() }}) as row_count
    {{ dq_empty_result_guard_sql() }}
{% endif %}
