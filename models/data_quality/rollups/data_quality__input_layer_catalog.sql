{{ config(
     enabled = var('enable_data_quality', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'input_layer_catalog',
     tags = ['data_quality', 'dq', 'dq1', 'dq_rollup'],
     materialized = 'table'
   )
}}

{% set input_layer_model_names = dq_enabled_input_layer_model_names() %}

{% for model_name in input_layer_model_names %}
-- depends_on: {{ ref(model_name) }}
{% endfor %}

{% if execute %}
    {% set catalog_queries = [] %}

    {% for model_node in dq_expected_input_layer_models() %}
        {% set table_name = model_node.name | replace('input_layer__', '') %}

        {% for expected_column in dq_expected_columns(model_node) %}
            {% set expected_type = expected_column['data_type'] %}
            {% set query %}
                select
                      '{{ table_name }}' as input_table_name
                    , '{{ model_node.name }}' as input_model_name
                    , '{{ expected_column["name"] }}' as input_column_name
                    , '{{ dq_input_layer_table_type(table_name) }}' as input_table_type
                    , {% if expected_type is not none %}'{{ expected_type }}'{% else %}cast(null as {{ dbt.type_string() }}){% endif %} as expected_data_type
                    , {% if expected_type is not none %}'{{ dq_type_family(expected_type) }}'{% else %}cast(null as {{ dbt.type_string() }}){% endif %} as expected_type_family
                    , '{{ "yes" if expected_column["is_primary_key"] else "no" }}' as is_primary_key
                    , cast({{ expected_column['column_order'] }} as {{ dbt.type_int() }}) as column_order
            {% endset %}
            {% do catalog_queries.append(query) %}
        {% endfor %}
    {% endfor %}

    {% if catalog_queries | length > 0 %}
        select *
        from (
            {{ catalog_queries | join('\nunion all\n') }}
        ) as input_layer_catalog
    {% else %}
        select
              cast(null as {{ dbt.type_string() }}) as input_table_name
            , cast(null as {{ dbt.type_string() }}) as input_model_name
            , cast(null as {{ dbt.type_string() }}) as input_column_name
            , cast(null as {{ dbt.type_string() }}) as input_table_type
            , cast(null as {{ dbt.type_string() }}) as expected_data_type
            , cast(null as {{ dbt.type_string() }}) as expected_type_family
            , cast(null as {{ dbt.type_string() }}) as is_primary_key
            , cast(null as {{ dbt.type_int() }}) as column_order
        {{ dq_empty_result_guard_sql() }}
    {% endif %}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as input_table_name
        , cast(null as {{ dbt.type_string() }}) as input_model_name
        , cast(null as {{ dbt.type_string() }}) as input_column_name
        , cast(null as {{ dbt.type_string() }}) as input_table_type
        , cast(null as {{ dbt.type_string() }}) as expected_data_type
        , cast(null as {{ dbt.type_string() }}) as expected_type_family
        , cast(null as {{ dbt.type_string() }}) as is_primary_key
        , cast(null as {{ dbt.type_int() }}) as column_order
    {{ dq_empty_result_guard_sql() }}
{% endif %}
