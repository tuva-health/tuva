{% macro limit_zero() -%}
    {% if adapter.type == 'fabric' %}
        {# No limit for Fabric #}
        {{ return('') }}
    {% else %}
        {{ adapter.dispatch('limit_zero', 'the_tuva_project')() }}
    {% endif %}
{%- endmacro %}

{% macro default__limit_zero() -%}
    limit 0
{%- endmacro %}

{% macro fabric__limit_zero() -%}
    {# No limit statement for Fabric #}
{%- endmacro %}

{# SQL Server is T-SQL. dbt-sqlserver registers no adapter-type parent,
   so fabric__ macros are not reached by dispatch and must be aliased. #}
{% macro sqlserver__limit_zero() -%}
    {{ the_tuva_project.fabric__limit_zero() }}
{%- endmacro %}
