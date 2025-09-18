{% macro varchar() -%}
    {{ adapter.dispatch('varchar')() }}
{%- endmacro %}

{% macro default__varchar() -%}
    VARCHAR
{%- endmacro %}

{% macro databricks__varchar() -%}
    VARCHAR(255)
{%- endmacro %}
