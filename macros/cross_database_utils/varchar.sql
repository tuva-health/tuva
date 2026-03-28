{% macro varchar() -%}
    {{ adapter.dispatch('varchar', 'the_tuva_project')() }}
{%- endmacro %}

{% macro default__varchar() -%}
    VARCHAR
{%- endmacro %}

{% macro databricks__varchar() -%}
    VARCHAR(255)
{%- endmacro %}

{% macro clickhouse__varchar() -%}
    Nullable(String)
{%- endmacro %}
