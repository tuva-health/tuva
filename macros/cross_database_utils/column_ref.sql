{% macro column_ref(relation_alias, column_name) -%}
    {{ adapter.dispatch('column_ref') (relation_alias, column_name) }}
{%- endmacro %}

{% macro fabric__column_ref(relation_alias, column_name) -%}
    {{ relation_alias }}.[{{ column_name }}]
{%- endmacro %}

{% macro default__column_ref(relation_alias, column_name) -%}
    {{ relation_alias }}.{{ column_name }}
{%- endmacro %}
