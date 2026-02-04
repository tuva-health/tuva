{% macro yyyymm(date) -%}
    {{ adapter.dispatch('yyyymm') (date) }}
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
