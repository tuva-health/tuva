-- based off of  https://github.com/calogica/dbt-date/blob/0.7.1/macros/calendar_date/date_part.sql

{% macro date_part(datepart, date) -%}
    {{ adapter.dispatch('date_part') (datepart, date) }}
{%- endmacro %}

{% macro default__date_part(datepart, date) -%}
    date_part('{{ datepart }}', {{  date }})
{%- endmacro %}

{% macro bigquery__date_part(datepart, date) -%}
    extract({{ datepart }} from {{ date }})
{%- endmacro %}