{% macro substring(string, start, length) %}
  {{ return(adapter.dispatch('substring', 'macros')(string, start, length)) }}
{% endmacro %}

{% macro default__substring(string, start, length) %}
  substring({{ string }}, {{ start }}, {{ length }})
{% endmacro %}

{% macro postgres__substring(string, start, length) %}
  substring({{ string }} from {{ start }} for {{ length }})
{% endmacro %}

{% macro snowflake__substring(string, start, length) %}
  substr({{ string }}, {{ start }}, {{ length }})
{% endmacro %}

{% macro bigquery__substring(string, start, length) %}
  substr({{ string }}, {{ start }}, {{ length }})
{% endmacro %}

{% macro redshift__substring(string, start, length) %}
  substring({{ string }}, {{ start }}, {{ length }})
{% endmacro %}

{% macro databricks__substring(string, start, length) %}
  substring({{ string }}, {{ start }}, {{ length }})
{% endmacro %}

{% macro duckdb__substring(string, start, length) %}
  substring({{ string }}, {{ start }}, {{ length }})
{% endmacro %}