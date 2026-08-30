{% macro dq_string_literal_sql(value) %}
    {% if value is none %}
        {{ return("cast(null as " ~ dbt.type_string() ~ ")") }}
    {% endif %}

    {% set escaped = (value | string)
        | replace('\\', '\\\\')
        | replace('\n', ' ')
        | replace('\r', ' ')
        | replace("'", "''")
    %}
    {{ return("'" ~ escaped ~ "'") }}
{% endmacro %}

{% macro dq_input_layer_table_type(table_name) %}
    {% set claims_tables = ['eligibility', 'medical_claim', 'pharmacy_claim'] %}
    {% set other_tables = ['provider_attribution'] %}

    {% if table_name in claims_tables %}
        {{ return('Claims') }}
    {% elif table_name in other_tables %}
        {{ return('Other') }}
    {% else %}
        {{ return('Clinical') }}
    {% endif %}
{% endmacro %}
