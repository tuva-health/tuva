{#

    Aggregate that is true when at least one non-null input row is true.

    Fabric has no boolean aggregate, so it takes the maximum of the bit
    column widened to int and narrows the result back to bit.

#}

{% macro bool_or_agg(expression) %}
  {{ return(adapter.dispatch('bool_or_agg', 'the_tuva_project')(expression)) }}
{% endmacro %}

{% macro default__bool_or_agg(expression) %}
  bool_or( {{ expression }} )
{% endmacro %}

{% macro snowflake__bool_or_agg(expression) %}
  boolor_agg( {{ expression }} )
{% endmacro %}

{% macro bigquery__bool_or_agg(expression) %}
  logical_or( {{ expression }} )
{% endmacro %}

{% macro fabric__bool_or_agg(expression) %}
  cast( max( cast( {{ expression }} as int) ) as bit)
{% endmacro %}

{# SQL Server is T-SQL. dbt-sqlserver registers no adapter-type parent,
   so fabric__ macros are not reached by dispatch and must be aliased. #}
{% macro sqlserver__bool_or_agg(expression) -%}
    {{ the_tuva_project.fabric__bool_or_agg(expression) }}
{%- endmacro %}
