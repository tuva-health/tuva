{#
    Override athena__concat to cast all fields to varchar before concatenating
#}
{% macro concat(fields) -%}
    {{ return(adapter.dispatch('concat')(fields)) }}
{%- endmacro %}

{% macro default__concat(fields) -%}
    {{ return(dbt_utils.concat(fields)) }}
{%- endmacro %}

{% macro athena__concat(fields) %}
    {% for field in fields %}
        cast({{ field }} as varchar) {{ ' || ' if not loop.last }}
    {% endfor %}
{% endmacro %}
