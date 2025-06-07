{% macro limit_zero() -%}
    {% if adapter.type == 'fabric' %}
        {# No limit for Fabric #}
        {{ return('') }}
    {% else %}
        {{ adapter.dispatch('limit_zero')() }}
    {% endif %}
{%- endmacro %}

{% macro default__limit_zero() -%}
    limit 0
{%- endmacro %}

{% macro fabric__limit_zero() -%}
    {# No limit statement for Fabric #}
{%- endmacro %}