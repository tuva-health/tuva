{% macro yyyymm(date) -%}
    {{ adapter.dispatch('yyyymm', 'the_tuva_project') (date) }}
{%- endmacro %}

{% macro duckdb__yyyymm(date) -%}
    strftime('%Y%m', cast({{ date }} as date))
{%- endmacro %}

{% macro bigquery__yyyymm(date) -%}
    format_date('%Y%m', cast({{ date }} as date))
{%- endmacro %}

{% macro databricks__yyyymm(date) -%}
    date_format(cast({{ date }} as date), 'yyyyMM')
{%- endmacro %}

{% macro fabric__yyyymm(date) -%}
    cast(format(cast({{ date }} as date), 'yyyyMM') as varchar(4000))
{%- endmacro %}

{% macro default__yyyymm(date) -%}
    to_char({{ date }}, 'YYYYMM')
{%- endmacro %}

{# SQL Server is T-SQL. dbt-sqlserver registers no adapter-type parent,
   so fabric__ macros are not reached by dispatch and must be aliased. #}
{% macro sqlserver__yyyymm(date) -%}
    {{ the_tuva_project.fabric__yyyymm(date) }}
{%- endmacro %}


{# Athena/Trino has no to_char(); date_format uses MySQL-style patterns. #}
{% macro athena__yyyymm(date) -%}
    DATE_FORMAT(cast({{ date }} as date), '%Y%m')
{%- endmacro %}
