{{ config(
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'logical',
     tags = ['data_quality', 'dqi'],
     materialized = 'table'
   )
}}

{% set logical_rules = dq_logical_rules() %}
{% set dependency_names = [
    'input_layer__appointment',
    'input_layer__condition',
    'input_layer__eligibility',
    'input_layer__encounter',
    'input_layer__lab_result',
    'input_layer__medical_claim',
    'input_layer__pharmacy_claim',
    'input_layer__provider_attribution'
] %}

{% for dependency_name in dependency_names %}
-- depends_on: {{ ref(dependency_name) }}
{% endfor %}
-- depends_on: {{ ref('reference_data__calendar') }}

{% if execute %}
    {% set logical_queries = [] %}

    {% for rule in logical_rules %}
        {% set model_node = dq_find_model_node(rule['model_name']) %}
        {% set relation = dq_actual_relation(model_node) if model_node is not none else none %}

        {% if relation is not none %}
            {% set query = dq_render_logical_rule_sql(rule, relation) %}
            {% if query is not none %}
                {% do logical_queries.append(query) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% if logical_queries | length > 0 %}
        {{ logical_queries | join('\nunion all\n') }}
    {% else %}
        select
              cast(null as {{ dbt.type_string() }}) as data_source
            , cast(null as {{ dbt.type_string() }}) as test_name
            , cast(null as {{ dbt.type_numeric() }}) as test_result
        where 1 = 0
    {% endif %}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as test_name
        , cast(null as {{ dbt.type_numeric() }}) as test_result
    where 1 = 0
{% endif %}
