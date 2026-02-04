{#

    Cross-database string concatenation with safe casting for adapters
    that don't support NVARCHAR (e.g., Fabric).

#}

{% macro concat_strings(args) -%}
    {{ adapter.dispatch('concat_strings')(args) }}
{%- endmacro %}

{% macro fabric__concat_strings(args) -%}
    concat(
        {%- for arg in args -%}
            cast({{ arg }} as varchar(4000))
            {%- if not loop.last %}, {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}

{% macro default__concat_strings(args) -%}
    {{ dbt.concat(args) }}
{%- endmacro %}
