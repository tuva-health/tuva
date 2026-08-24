{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool)
       and ((var('claims_enabled', false) | as_bool) or (var('clinical_enabled', false) | as_bool)),
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

{#
  Logical Data Quality exposes an intentionally small public dbt interface.
  This graph assertion prevents internal flag models or a future Logical helper
  from becoming public accidentally. The optional failure-key model belongs to
  the allowlist only when it is enabled.
#}

{% set expected_public_model_names = [
    'data_quality__logical_test_catalog',
    'data_quality__logical_test_input_columns',
    'data_quality__logical_test_results'
] %}

{% if var('enable_data_quality_failure_keys', false) | as_bool %}
    {% do expected_public_model_names.append('data_quality__logical_failure_keys') %}
{% endif %}

{# These refs establish the same enablement-aware dependencies as the contract. #}
{% for expected_public_model_name in expected_public_model_names %}
    {% set expected_public_relation = ref(expected_public_model_name) %}
{% endfor %}

{% set seen_expected_public_model_names = [] %}
{% set access_failures = [] %}
{% set forbidden_model_names = ['data_quality__logical'] %}
{% set forbidden_legacy_tags = ['dq', 'dq1', 'dq2', 'dq_analytics', 'dq_analytical'] %}

{% if execute %}
    {% for graph_node in graph['nodes'].values() %}
        {% set node_tags = graph_node.config.tags if graph_node.config is not none else [] %}
        {% set matched_legacy_tags = [] %}
        {% for node_tag in node_tags %}
            {% if node_tag in forbidden_legacy_tags %}
                {% do matched_legacy_tags.append(node_tag) %}
            {% endif %}
        {% endfor %}

        {% if graph_node.package_name == 'the_tuva_project'
              and graph_node.resource_type == 'model'
              and graph_node.name in forbidden_model_names %}
            {% set forbidden_model_query %}
                select
                      {{ dq_string_literal_sql(graph_node.name) }} as model_name
                    , {{ dq_string_literal_sql('removed') }} as expected_access
                    , {{ dq_string_literal_sql(graph_node.config.access) }} as actual_access
                    , {{ dq_string_literal_sql('removed_logical_compatibility_model_exists') }} as issue_type
            {% endset %}
            {% do access_failures.append(forbidden_model_query | trim) %}
        {% endif %}

        {% if graph_node.package_name == 'the_tuva_project'
              and matched_legacy_tags | length > 0 %}
            {% set forbidden_tag_query %}
                select
                      {{ dq_string_literal_sql(graph_node.name) }} as model_name
                    , {{ dq_string_literal_sql('no_legacy_data_quality_tag') }} as expected_access
                    , {{ dq_string_literal_sql(matched_legacy_tags | join(',')) }} as actual_access
                    , {{ dq_string_literal_sql('removed_data_quality_selector_tag_exists') }} as issue_type
            {% endset %}
            {% do access_failures.append(forbidden_tag_query | trim) %}
        {% endif %}

        {% set is_logical_model =
            graph_node.package_name == 'the_tuva_project'
            and graph_node.resource_type == 'model'
            and graph_node.config.enabled
            and (
                graph_node.original_file_path.startswith('models/data_quality/logical/')
                or graph_node.name.startswith('data_quality__logical')
                or 'dq_logical' in node_tags
            )
        %}

        {% if is_logical_model %}
            {% set actual_access = graph_node.config.access %}

            {% if graph_node.name in expected_public_model_names %}
                {% do seen_expected_public_model_names.append(graph_node.name) %}
                {% if actual_access != 'public' %}
                    {% set missing_public_access_query %}
                        select
                              {{ dq_string_literal_sql(graph_node.name) }} as model_name
                            , {{ dq_string_literal_sql('public') }} as expected_access
                            , {{ dq_string_literal_sql(actual_access) }} as actual_access
                            , {{ dq_string_literal_sql('documented_interface_not_public') }} as issue_type
                    {% endset %}
                    {% do access_failures.append(missing_public_access_query | trim) %}
                {% endif %}
            {% elif actual_access == 'public' %}
                {% set unexpected_public_access_query %}
                    select
                          {{ dq_string_literal_sql(graph_node.name) }} as model_name
                        , {{ dq_string_literal_sql('not_public') }} as expected_access
                        , {{ dq_string_literal_sql(actual_access) }} as actual_access
                        , {{ dq_string_literal_sql('internal_logical_model_is_public') }} as issue_type
                {% endset %}
                {% do access_failures.append(unexpected_public_access_query | trim) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% for expected_public_model_name in expected_public_model_names %}
        {% if expected_public_model_name not in seen_expected_public_model_names %}
            {% set missing_model_query %}
                select
                      {{ dq_string_literal_sql(expected_public_model_name) }} as model_name
                    , {{ dq_string_literal_sql('public') }} as expected_access
                    , cast(null as {{ dbt.type_string() }}) as actual_access
                    , {{ dq_string_literal_sql('documented_interface_not_enabled') }} as issue_type
            {% endset %}
            {% do access_failures.append(missing_model_query | trim) %}
        {% endif %}
    {% endfor %}
{% endif %}

{% if access_failures | length > 0 %}
    {{ access_failures | join('\nunion all\n') }}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as model_name
        , cast(null as {{ dbt.type_string() }}) as expected_access
        , cast(null as {{ dbt.type_string() }}) as actual_access
        , cast(null as {{ dbt.type_string() }}) as issue_type
    {{ dq_empty_result_guard_sql() }}
{% endif %}
