{{ config(
     enabled = the_tuva_project.tuva_boolean_var('data_quality_enabled', false),
     severity = 'error',
     tags = ['data_quality']
   )
}}

{#
  Data Quality is organized around Structural and Logical models. This graph
  assertion prevents the removed downstream-rollup models and selector tag from
  being reintroduced accidentally.
#}

{% set removed_model_names = [
    'data_quality__data_source_catalog',
    'data_quality__domain_catalog',
    'data_quality__domain_input_requirements',
    'data_quality__component_test_quality_results',
    'data_quality__component_table_quality_results',
    'data_quality__component_quality_results',
    'data_quality__source_quality_overview'
] %}

{% set removal_failures = [] %}

{% if execute %}
    {% for graph_node in graph['nodes'].values() %}
        {% set node_tags = graph_node.config.tags if graph_node.config is not none else [] %}

        {% if graph_node.package_name == 'the_tuva_project'
              and graph_node.resource_type == 'model'
              and graph_node.name in removed_model_names %}
            {% set removed_model_query %}
                select
                      {{ dq_string_literal_sql(graph_node.name) }} as resource_name
                    , {{ dq_string_literal_sql('removed_data_quality_rollup_model_exists') }} as issue_type
            {% endset %}
            {% do removal_failures.append(removed_model_query | trim) %}
        {% endif %}

        {% if graph_node.package_name == 'the_tuva_project'
              and 'dq_rollup' in node_tags %}
            {% set removed_tag_query %}
                select
                      {{ dq_string_literal_sql(graph_node.name) }} as resource_name
                    , {{ dq_string_literal_sql('removed_dq_rollup_tag_exists') }} as issue_type
            {% endset %}
            {% do removal_failures.append(removed_tag_query | trim) %}
        {% endif %}
    {% endfor %}
{% endif %}

{% if removal_failures | length > 0 %}
    {{ removal_failures | join('\nunion all\n') }}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as resource_name
        , cast(null as {{ dbt.type_string() }}) as issue_type
    {{ dq_empty_result_guard_sql() }}
{% endif %}
