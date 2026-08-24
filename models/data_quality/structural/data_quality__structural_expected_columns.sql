{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_expected_columns',
     tags = ['data_quality', 'dq_structural'],
     materialized = 'table'
   )
}}

{% set input_layer_model_names = dq_enabled_input_layer_model_names() %}

{% for model_name in input_layer_model_names %}
-- depends_on: {{ ref(model_name) }}
{% endfor %}

{% if execute %}
    {% set expected_queries = [] %}
    {% set missing_data_type_columns = [] %}

    {% for model_node in dq_expected_input_layer_models() %}
        {% set table_name = model_node.name | replace('input_layer__', '') %}
        {% set input_layer_domain = dq_input_layer_domain_name(model_node.name) %}

        {% for expected_column in dq_expected_columns(model_node) %}
            {% set expected_name = expected_column['name'] %}
            {% set expected_type = expected_column['data_type'] %}

            {% if expected_type is none %}
                {% do missing_data_type_columns.append(model_node.name ~ '.' ~ expected_name) %}
            {% endif %}

            {% set query %}
                select
                      '{{ input_layer_domain }}' as input_layer_domain
                    , '{{ table_name }}' as table_name
                    , '{{ model_node.name }}' as model_name
                    , '{{ expected_name }}' as column_name
                    , {% if expected_type is not none %}'{{ expected_type }}'{% else %}cast(null as {{ dbt.type_string() }}){% endif %} as expected_data_type
                    , {% if expected_type is not none %}'{{ dq_type_family(expected_type) }}'{% else %}cast(null as {{ dbt.type_string() }}){% endif %} as expected_type_family
                    , '{{ "yes" if expected_column["is_primary_key"] else "no" }}' as is_primary_key
                    , cast({{ expected_column['column_order'] }} as {{ dbt.type_int() }}) as column_order
            {% endset %}

            {% do expected_queries.append(query) %}
        {% endfor %}
    {% endfor %}

    {% if missing_data_type_columns | length > 0 %}
        {{ exceptions.raise_compiler_error(
            "Missing meta.data_type for Input Layer columns: " ~ (missing_data_type_columns | join(', '))
        ) }}
    {% endif %}

    {% if expected_queries | length > 0 %}
        select *
        from (
            {{ expected_queries | join('\nunion all\n') }}
        ) as structural_expected_columns
    {% else %}
        select
              cast(null as {{ dbt.type_string() }}) as input_layer_domain
            , cast(null as {{ dbt.type_string() }}) as table_name
            , cast(null as {{ dbt.type_string() }}) as model_name
            , cast(null as {{ dbt.type_string() }}) as column_name
            , cast(null as {{ dbt.type_string() }}) as expected_data_type
            , cast(null as {{ dbt.type_string() }}) as expected_type_family
            , cast(null as {{ dbt.type_string() }}) as is_primary_key
            , cast(null as {{ dbt.type_int() }}) as column_order
        {{ dq_empty_result_guard_sql() }}
    {% endif %}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as input_layer_domain
        , cast(null as {{ dbt.type_string() }}) as table_name
        , cast(null as {{ dbt.type_string() }}) as model_name
        , cast(null as {{ dbt.type_string() }}) as column_name
        , cast(null as {{ dbt.type_string() }}) as expected_data_type
        , cast(null as {{ dbt.type_string() }}) as expected_type_family
        , cast(null as {{ dbt.type_string() }}) as is_primary_key
        , cast(null as {{ dbt.type_int() }}) as column_order
    {{ dq_empty_result_guard_sql() }}
{% endif %}
