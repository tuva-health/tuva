{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

{#
  `tag:dq_logical` is the supported narrow selector for Logical Data Quality.
  Any selected Logical model or test that depends on another Data Quality
  model must bring that parent into the same selector.  Input Layer and
  terminology parents are deliberately outside this assertion because users
  build those prerequisites before Logical Data Quality.
#}

{% set closure_failures = [] %}

{% if execute %}
    {% for graph_node in graph['nodes'].values() %}
        {% set node_tags = graph_node.config.tags if graph_node.config is not none else [] %}
        {% if graph_node.package_name == 'the_tuva_project'
              and graph_node.config.enabled
              and 'dq_logical' in node_tags %}
            {% for dependency_id in graph_node.depends_on.nodes %}
                {% set dependency = graph['nodes'].get(dependency_id) %}
                {% if dependency is not none
                      and dependency.package_name == 'the_tuva_project'
                      and dependency.resource_type == 'model'
                      and dependency.config.enabled
                      and dependency.original_file_path.startswith('models/data_quality/') %}
                    {% set dependency_tags = dependency.config.tags if dependency.config is not none else [] %}
                    {% if 'dq_logical' not in dependency_tags %}
                        {% set failure_query %}
                            select
                                  {{ dq_string_literal_sql(graph_node.name) }} as selected_node_name
                                , {{ dq_string_literal_sql(dependency.name) }} as unselected_parent_name
                        {% endset %}
                        {% do closure_failures.append(failure_query | trim) %}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endif %}

{% if closure_failures | length > 0 %}
    {{ closure_failures | join('\nunion all\n') }}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as selected_node_name
        , cast(null as {{ dbt.type_string() }}) as unselected_parent_name
    {{ dq_empty_result_guard_sql() }}
{% endif %}
