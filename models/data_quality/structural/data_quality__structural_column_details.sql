{{ config(
     enabled = var('enable_data_quality', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_column_details',
     tags = ['data_quality', 'dq', 'dq1', 'dq_structural'],
     materialized = 'table'
   )
}}

{% set claim_model_names = dq_claims_structural_model_names() %}

{% for model_name in claim_model_names %}
-- depends_on: {{ ref(model_name) }}
{% endfor %}

{% if execute %}
    {% set detail_queries = [] %}

    {% for model_name in claim_model_names %}
        {% set model_node = dq_find_model_node(model_name) %}

        {% if model_node is not none %}
            {% set table_name = model_name | replace('input_layer__', '') %}
            {% set relation = dq_actual_relation(model_node) %}
            {% set expected_columns = dq_expected_columns(model_node) %}
            {% set actual_column_types = {} %}

            {% if relation is not none %}
                {% for column in dq_actual_columns(relation) %}
                    {% do actual_column_types.update({column.name | lower: column.dtype}) %}
                {% endfor %}
                {% set source_dimension_sql = dq_source_dimension_sql(relation) %}
            {% else %}
                {% set source_dimension_sql = dq_missing_source_dimension_sql() %}
            {% endif %}

            {% for expected_column in expected_columns %}
                {% set expected_name = expected_column['name'] %}
                {% set expected_type = expected_column['data_type'] %}
                {% set actual_type = actual_column_types.get(expected_name) %}
                {% set column_exists = 'yes' if actual_type is not none else 'no' %}

                {% if actual_type is none %}
                    {% set data_type_correct = 'no' %}
                {% elif expected_type is none or dq_type_families_match(expected_type, actual_type) %}
                    {% set data_type_correct = 'yes' %}
                {% else %}
                    {% set data_type_correct = 'no' %}
                {% endif %}

                {% set query %}
                    select
                          sources.data_source
                        , '{{ table_name }}' as {{ adapter.quote('table') }}
                        , '{{ expected_name }}' as {{ adapter.quote('column') }}
                        , '{{ column_exists }}' as column_exists
                        , '{{ data_type_correct }}' as data_type_correct
                    from (
                        {{ source_dimension_sql }}
                    ) as sources
                {% endset %}

                {% do detail_queries.append(query) %}
            {% endfor %}
        {% endif %}
    {% endfor %}

    select *
    from (
        {{ detail_queries | join('\nunion all\n') }}
    ) as structural_column_details
    order by 1, 2, 3
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as {{ adapter.quote('table') }}
        , cast(null as {{ dbt.type_string() }}) as {{ adapter.quote('column') }}
        , cast(null as {{ dbt.type_string() }}) as column_exists
        , cast(null as {{ dbt.type_string() }}) as data_type_correct
    where 1 = 0
{% endif %}
