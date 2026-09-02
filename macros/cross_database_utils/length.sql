{% macro length(col) %}
  {{ return(adapter.dispatch('length', 'the_tuva_project')(col)) }}
{% endmacro %}

{% macro default__length(col) %}
  length( {{ col }} )
{% endmacro %}

{% macro fabric__length(col) %}
  len( {{ col }} )
{% endmacro %}

{# SQL Server is T-SQL. dbt-sqlserver registers no adapter-type parent,
   so fabric__ macros are not reached by dispatch and must be aliased. #}
{% macro sqlserver__length(col) -%}
    {{ the_tuva_project.fabric__length(col) }}
{%- endmacro %}
