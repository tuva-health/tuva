{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool)
       and ((var('claims_enabled', false) | as_bool) or (var('clinical_enabled', false) | as_bool)),
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

{#
  These four relations are Logical Data Quality's stable public interfaces.
  Keep their physical schemas exact so an implementation helper cannot leak
  into the 1.x contract and a documented field cannot disappear unnoticed.
#}

{% set public_relations = [
    {
        'model_name': 'data_quality__logical_test_catalog',
        'expected_columns': [
            'test_name', 'display_name', 'description', 'input_model_name',
            'input_table_name', 'flag_model_name', 'flag_table_name',
            'flag_column_name', 'grain', 'key_columns', 'test_type', 'severity'
        ],
        'expected_types': {
            'test_name': 'string',
            'display_name': 'string',
            'description': 'string',
            'input_model_name': 'string',
            'input_table_name': 'string',
            'flag_model_name': 'string',
            'flag_table_name': 'string',
            'flag_column_name': 'string',
            'grain': 'string',
            'key_columns': 'string',
            'test_type': 'string',
            'severity': 'integer'
        }
    },
    {
        'model_name': 'data_quality__logical_test_input_columns',
        'expected_columns': ['test_name', 'input_table_name', 'input_column_name'],
        'expected_types': {
            'test_name': 'string',
            'input_table_name': 'string',
            'input_column_name': 'string'
        }
    },
    {
        'model_name': 'data_quality__logical_test_results',
        'expected_columns': [
            'data_source', 'input_table_name', 'test_name', 'display_name',
            'description', 'grain', 'flag_table_name', 'flag_column_name',
            'test_type', 'severity', 'total_row_count', 'tested_count',
            'failed_count', 'passed_count', 'not_applicable_count'
        ],
        'expected_types': {
            'data_source': 'string',
            'input_table_name': 'string',
            'test_name': 'string',
            'display_name': 'string',
            'description': 'string',
            'grain': 'string',
            'flag_table_name': 'string',
            'flag_column_name': 'string',
            'test_type': 'string',
            'severity': 'integer',
            'total_row_count': 'bigint',
            'tested_count': 'bigint',
            'failed_count': 'bigint',
            'passed_count': 'bigint',
            'not_applicable_count': 'bigint'
        }
    }
] %}

{% if var('enable_data_quality_failure_keys', false) | as_bool %}
    {% do public_relations.append({
        'model_name': 'data_quality__logical_failure_keys',
        'expected_columns': [
            'data_source', 'input_table_name', 'test_name', 'grain',
            'key_columns', 'key_values_format', 'key_values'
        ],
        'expected_types': {
            'data_source': 'string',
            'input_table_name': 'string',
            'test_name': 'string',
            'grain': 'string',
            'key_columns': 'string',
            'key_values_format': 'string',
            'key_values': 'string'
        }
    }) %}
{% endif %}

{% set mismatch_queries = [] %}

{% for public_relation in public_relations %}
    {# Calling ref outside the execute guard records the graph dependency. #}
    {% set relation = ref(public_relation['model_name']) %}

    {% if execute %}
        {% set actual_columns = [] %}
        {% set actual_column_types = {} %}
        {% for actual_column in adapter.get_columns_in_relation(relation) %}
            {% set normalized_column_name = actual_column.name | lower %}
            {% if normalized_column_name not in actual_columns %}
                {% do actual_columns.append(normalized_column_name) %}
                {% do actual_column_types.update({normalized_column_name: actual_column.data_type}) %}
            {% endif %}
        {% endfor %}

        {% for expected_column in public_relation['expected_columns'] %}
            {% if expected_column not in actual_columns %}
                {% set missing_query %}
                    select
                          {{ dq_string_literal_sql(public_relation['model_name']) }} as public_model_name
                        , {{ dq_string_literal_sql(expected_column) }} as expected_column_name
                        , cast(null as {{ dbt.type_string() }}) as actual_column_name
                        , cast({{ loop.index }} as {{ dbt.type_int() }}) as column_position
                        , {{ dq_string_literal_sql('missing_public_column') }} as mismatch_type
                        , {{ dq_string_literal_sql(public_relation['expected_types'][expected_column]) }} as expected_data_type
                        , cast(null as {{ dbt.type_string() }}) as actual_data_type
                {% endset %}
                {% do mismatch_queries.append(missing_query | trim) %}
            {% endif %}
        {% endfor %}

        {% for actual_column in actual_columns %}
            {% if actual_column not in public_relation['expected_columns'] %}
                {% set extra_query %}
                    select
                          {{ dq_string_literal_sql(public_relation['model_name']) }} as public_model_name
                        , cast(null as {{ dbt.type_string() }}) as expected_column_name
                        , {{ dq_string_literal_sql(actual_column) }} as actual_column_name
                        , cast({{ loop.index }} as {{ dbt.type_int() }}) as column_position
                        , {{ dq_string_literal_sql('unexpected_public_column') }} as mismatch_type
                        , cast(null as {{ dbt.type_string() }}) as expected_data_type
                        , {{ dq_string_literal_sql(actual_column_types[actual_column]) }} as actual_data_type
                {% endset %}
                {% do mismatch_queries.append(extra_query | trim) %}
            {% endif %}
        {% endfor %}

        {% set sorted_actual_columns = actual_columns | sort %}
        {% set sorted_expected_columns = public_relation['expected_columns'] | sort %}
        {% if sorted_actual_columns == sorted_expected_columns %}
            {% for expected_column in public_relation['expected_columns'] %}
                {% set actual_column = actual_columns[loop.index0] %}
                {% if actual_column != expected_column %}
                    {% set order_query %}
                        select
                              {{ dq_string_literal_sql(public_relation['model_name']) }} as public_model_name
                            , {{ dq_string_literal_sql(expected_column) }} as expected_column_name
                            , {{ dq_string_literal_sql(actual_column) }} as actual_column_name
                            , cast({{ loop.index }} as {{ dbt.type_int() }}) as column_position
                            , {{ dq_string_literal_sql('public_column_order_mismatch') }} as mismatch_type
                            , {{ dq_string_literal_sql(public_relation['expected_types'][expected_column]) }} as expected_data_type
                            , {{ dq_string_literal_sql(actual_column_types[actual_column]) }} as actual_data_type
                    {% endset %}
                    {% do mismatch_queries.append(order_query | trim) %}
                {% endif %}
            {% endfor %}
        {% endif %}

        {% for expected_column in public_relation['expected_columns'] %}
            {% if expected_column in actual_column_types %}
                {% set expected_data_type = public_relation['expected_types'][expected_column] %}
                {% set actual_data_type = actual_column_types[expected_column] %}
                {% set type_matches =
                    (expected_data_type == 'string' and dq_type_family(actual_data_type) == 'string')
                    or (expected_data_type == 'integer' and dq_type_family(actual_data_type) == 'integer')
                    or (expected_data_type == 'bigint' and dq_type_has_64_bit_integer_capacity(actual_data_type))
                %}

                {% if not type_matches %}
                    {% set type_query %}
                        select
                              {{ dq_string_literal_sql(public_relation['model_name']) }} as public_model_name
                            , {{ dq_string_literal_sql(expected_column) }} as expected_column_name
                            , {{ dq_string_literal_sql(expected_column) }} as actual_column_name
                            , cast({{ loop.index }} as {{ dbt.type_int() }}) as column_position
                            , {{ dq_string_literal_sql('public_column_type_mismatch') }} as mismatch_type
                            , {{ dq_string_literal_sql(expected_data_type) }} as expected_data_type
                            , {{ dq_string_literal_sql(actual_data_type) }} as actual_data_type
                    {% endset %}
                    {% do mismatch_queries.append(type_query | trim) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endfor %}

{% if mismatch_queries | length > 0 %}
    {{ mismatch_queries | join('\nunion all\n') }}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as public_model_name
        , cast(null as {{ dbt.type_string() }}) as expected_column_name
        , cast(null as {{ dbt.type_string() }}) as actual_column_name
        , cast(null as {{ dbt.type_int() }}) as column_position
        , cast(null as {{ dbt.type_string() }}) as mismatch_type
        , cast(null as {{ dbt.type_string() }}) as expected_data_type
        , cast(null as {{ dbt.type_string() }}) as actual_data_type
    {{ dq_empty_result_guard_sql() }}
{% endif %}
