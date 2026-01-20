{#
    This macro formats a date or timestamp column as 'YYYYMM' string.
    Cross-database compatible replacement for TO_CHAR(date, 'YYYYMM').
    Handles both DATE and TIMESTAMP/DATETIME types.

    Usage: {{ year_month('date_column') }}
    Output: '202401' (for January 2024)
#}

{% macro year_month(date_column) -%}
    {{ return(adapter.dispatch('year_month')(date_column)) }}
{%- endmacro %}

{% macro default__year_month(date_column) -%}
    TO_CHAR(try_cast({{ date_column }} as date), 'YYYYMM')
{%- endmacro %}

{% macro bigquery__year_month(date_column) -%}
    FORMAT_DATE('%Y%m', safe_cast({{ date_column }} as date))
{%- endmacro %}

{% macro postgres__year_month(date_column) -%}
    TO_CHAR(cast({{ date_column }} as date), 'YYYYMM')
{%- endmacro %}

{% macro redshift__year_month(date_column) -%}
    TO_CHAR(cast({{ date_column }} as date), 'YYYYMM')
{%- endmacro %}

{% macro snowflake__year_month(date_column) -%}
    TO_CHAR(try_cast({{ date_column }} as date), 'YYYYMM')
{%- endmacro %}

{% macro databricks__year_month(date_column) -%}
    DATE_FORMAT(cast({{ date_column }} as date), 'yyyyMM')
{%- endmacro %}

{% macro duckdb__year_month(date_column) -%}
    STRFTIME(cast({{ date_column }} as date), '%Y%m')
{%- endmacro %}

{% macro athena__year_month(date_column) -%}
    DATE_FORMAT(cast({{ date_column }} as date), '%Y%m')
{%- endmacro %}

{% macro fabric__year_month(date_column) -%}
    FORMAT(cast({{ date_column }} as date), 'yyyyMM')
{%- endmacro %}
