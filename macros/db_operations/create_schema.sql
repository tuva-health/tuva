{% macro create_schema(schema_name) %}
  {% set sql %}
    CREATE SCHEMA IF NOT EXISTS {{ schema_name }};
  {% endset %}
  {% do run_query(sql) %}
{% endmacro %}