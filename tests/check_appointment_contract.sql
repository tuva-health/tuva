{{ config(
     enabled = the_tuva_project.tuva_boolean_var('clinical_enabled', false),
     severity = 'error',
     tags = ['contract', 'appointment_contract']
   )
}}

{#
  Appointment type, status, reason, and cancellation_reason are source-EHR
  varchar descriptions. The former code/description pairs and normalized
  terminology fields must not reappear in the Input, Normalized, or Core
  appointment contracts.
#}

{% set required_columns = {
    'type': 'string',
    'status': 'string',
    'reason': 'string',
    'cancellation_reason': 'string',
    'duration': 'integer'
} %}
{% set forbidden_columns = [
    'type_code',
    'type_description',
    'type_code_norm',
    'type_description_norm',
    'status_code',
    'status_description',
    'status_code_norm',
    'status_description_norm',
    'cancellation_reason_code_norm',
    'cancellation_reason_description_norm'
] %}
{% set contract_model_names = [
    'input_layer__appointment',
    'normalized__appointment',
    'core__appointment'
] %}
{% set mismatch_queries = [] %}

{% for model_name in contract_model_names %}
    {% set relation = ref(model_name) %}

    {% if execute %}
        {% set actual_column_types = {} %}
        {% for actual_column in adapter.get_columns_in_relation(relation) %}
            {% do actual_column_types.update({actual_column.name | lower: actual_column.data_type}) %}
        {% endfor %}

        {% for column_name, expected_type_family in required_columns.items() %}
            {% if column_name not in actual_column_types %}
                {% set mismatch_query %}
                    select
                          {{ dq_string_literal_sql(model_name) }} as public_model_name
                        , {{ dq_string_literal_sql(column_name) }} as column_name
                        , {{ dq_string_literal_sql('missing_required_column') }} as mismatch_type
                        , cast(null as {{ dbt.type_string() }}) as actual_value
                {% endset %}
                {% do mismatch_queries.append(mismatch_query | trim) %}
            {% elif dq_type_family(actual_column_types[column_name]) != expected_type_family %}
                {% set mismatch_query %}
                    select
                          {{ dq_string_literal_sql(model_name) }} as public_model_name
                        , {{ dq_string_literal_sql(column_name) }} as column_name
                        , {{ dq_string_literal_sql('unexpected_physical_type') }} as mismatch_type
                        , {{ dq_string_literal_sql(actual_column_types[column_name]) }} as actual_value
                {% endset %}
                {% do mismatch_queries.append(mismatch_query | trim) %}
            {% endif %}
        {% endfor %}

        {% for column_name in forbidden_columns %}
            {% if column_name in actual_column_types %}
                {% set mismatch_query %}
                    select
                          {{ dq_string_literal_sql(model_name) }} as public_model_name
                        , {{ dq_string_literal_sql(column_name) }} as column_name
                        , {{ dq_string_literal_sql('retired_column_present') }} as mismatch_type
                        , {{ dq_string_literal_sql(actual_column_types[column_name]) }} as actual_value
                {% endset %}
                {% do mismatch_queries.append(mismatch_query | trim) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endfor %}

{% if mismatch_queries | length > 0 %}
    {{ mismatch_queries | join('\nunion all\n') }}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as public_model_name
        , cast(null as {{ dbt.type_string() }}) as column_name
        , cast(null as {{ dbt.type_string() }}) as mismatch_type
        , cast(null as {{ dbt.type_string() }}) as actual_value
    where 1 = 0
{% endif %}
