{#
    Custom concat macro that casts all fields to varchar before concatenating
#}
{% macro concat_custom(fields) -%}
    {{ return(adapter.dispatch('concat_custom')(fields)) }}
{%- endmacro %}

{% macro default__concat_custom(fields) -%}
    {{ return(dbt.concat(fields)) }}
{%- endmacro %}

{% macro athena__concat_custom(fields) %}
    {% for field in fields %}
        cast({{ field }} as varchar) {{ ' || ' if not loop.last }}
    {% endfor %}
{% endmacro %}