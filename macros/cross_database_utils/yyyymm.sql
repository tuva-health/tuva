{% macro yyyymm(date) -%}
    {{ adapter.dispatch('yyyymm') (date) }}
{%- endmacro %}

{% macro duckdb__yyyymm(date) -%}
    strftime('%Y%m', cast({{ date }} as date))
{%- endmacro %}

{% macro default__yyyymm(date) -%}
    to_char({{ date }}, 'YYYYMM')
{%- endmacro %}
