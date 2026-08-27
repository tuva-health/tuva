{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

{#
  The registry is the complete contract for every final flag relation.  This
  test compares that contract with the columns the adapter reports after each
  relation has been built.  It therefore catches unregistered outputs,
  missing registered keys or flags, incorrect physical order, and non-integer
  flag types without scanning any flag rows.
#}

{% set definitions_by_model = {} %}
{% set mismatch_queries = [] %}

{% for definition in dq_enabled_logical_test_manifest() %}
    {% set source_model_name = definition['source_model_name'] %}
    {% if definitions_by_model.get(source_model_name) is none %}
        {% do definitions_by_model.update({source_model_name: []}) %}
    {% endif %}
    {% do definitions_by_model[source_model_name].append(definition) %}
{% endfor %}

{% for source_model_name, model_definitions in definitions_by_model.items() %}
    {# Calling ref outside the execute guard records the graph dependency. #}
    {% set flag_relation = ref(source_model_name) %}

    {% if execute %}
        {% set expected_columns = [] %}
        {% for key_column in model_definitions[0]['key_columns'] %}
            {% set normalized_key_column = key_column | lower %}
            {% if normalized_key_column not in expected_columns %}
                {% do expected_columns.append(normalized_key_column) %}
            {% endif %}
        {% endfor %}
        {% for definition in model_definitions %}
            {% set normalized_flag_column = definition['flag_column_name'] | lower %}
            {% if normalized_flag_column not in expected_columns %}
                {% do expected_columns.append(normalized_flag_column) %}
            {% endif %}
        {% endfor %}

        {% set actual_columns = [] %}
        {% set actual_column_types = {} %}
        {% for actual_column in adapter.get_columns_in_relation(flag_relation) %}
            {% set normalized_actual_column = actual_column.name | lower %}
            {% if normalized_actual_column not in actual_columns %}
                {% do actual_columns.append(normalized_actual_column) %}
                {% do actual_column_types.update({normalized_actual_column: actual_column.data_type}) %}
            {% endif %}
        {% endfor %}

        {% for expected_column in expected_columns %}
            {% if expected_column not in actual_columns %}
                {% set missing_query %}
                    select
                          {{ dq_string_literal_sql(source_model_name) }} as flag_model_name
                        , {{ dq_string_literal_sql(expected_column) }} as expected_column_name
                        , cast(null as {{ dbt.type_string() }}) as actual_column_name
                        , {{ dq_string_literal_sql('missing_registered_column') }} as mismatch_type
                        , cast(null as {{ dbt.type_string() }}) as actual_data_type
                {% endset %}
                {% do mismatch_queries.append(missing_query | trim) %}
            {% endif %}
        {% endfor %}

        {% for actual_column in actual_columns %}
            {% if actual_column not in expected_columns %}
                {% set extra_query %}
                    select
                          {{ dq_string_literal_sql(source_model_name) }} as flag_model_name
                        , cast(null as {{ dbt.type_string() }}) as expected_column_name
                        , {{ dq_string_literal_sql(actual_column) }} as actual_column_name
                        , {{ dq_string_literal_sql('unregistered_output_column') }} as mismatch_type
                        , {{ dq_string_literal_sql(actual_column_types[actual_column]) }} as actual_data_type
                {% endset %}
                {% do mismatch_queries.append(extra_query | trim) %}
            {% endif %}
        {% endfor %}

        {% set sorted_actual_columns = actual_columns | sort %}
        {% set sorted_expected_columns = expected_columns | sort %}
        {% if sorted_actual_columns == sorted_expected_columns %}
            {% for expected_column in expected_columns %}
                {% set actual_column = actual_columns[loop.index0] %}
                {% if actual_column != expected_column %}
                    {% set order_query %}
                        select
                              {{ dq_string_literal_sql(source_model_name) }} as flag_model_name
                            , {{ dq_string_literal_sql(expected_column) }} as expected_column_name
                            , {{ dq_string_literal_sql(actual_column) }} as actual_column_name
                            , {{ dq_string_literal_sql('flag_column_order_mismatch') }} as mismatch_type
                            , {{ dq_string_literal_sql(actual_column_types[actual_column]) }} as actual_data_type
                    {% endset %}
                    {% do mismatch_queries.append(order_query | trim) %}
                {% endif %}
            {% endfor %}
        {% endif %}

        {% for definition in model_definitions %}
            {% set flag_column_name = definition['flag_column_name'] | lower %}
            {% if flag_column_name in actual_column_types
                  and dq_type_family(actual_column_types[flag_column_name]) != 'integer' %}
                {% set type_query %}
                    select
                          {{ dq_string_literal_sql(source_model_name) }} as flag_model_name
                        , {{ dq_string_literal_sql(flag_column_name) }} as expected_column_name
                        , {{ dq_string_literal_sql(flag_column_name) }} as actual_column_name
                        , {{ dq_string_literal_sql('registered_flag_non_integer_type') }} as mismatch_type
                        , {{ dq_string_literal_sql(actual_column_types[flag_column_name]) }} as actual_data_type
                {% endset %}
                {% do mismatch_queries.append(type_query | trim) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endfor %}

{% if mismatch_queries | length > 0 %}
    {{ mismatch_queries | join('\nunion all\n') }}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as flag_model_name
        , cast(null as {{ dbt.type_string() }}) as expected_column_name
        , cast(null as {{ dbt.type_string() }}) as actual_column_name
        , cast(null as {{ dbt.type_string() }}) as mismatch_type
        , cast(null as {{ dbt.type_string() }}) as actual_data_type
    {{ dq_empty_result_guard_sql() }}
{% endif %}
