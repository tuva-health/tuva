{#
    Emits the adapter-specific spelling for a duplicate-eliminating set union.

    BigQuery requires the DISTINCT keyword, while Fabric's T-SQL grammar
    rejects it because bare UNION is already duplicate-eliminating.
#}
{% macro union_distinct() -%}
    {{ return(adapter.dispatch('union_distinct', 'the_tuva_project')()) }}
{%- endmacro %}

{% macro default__union_distinct() -%}
    union distinct
{%- endmacro %}

{% macro fabric__union_distinct() -%}
    union
{%- endmacro %}
