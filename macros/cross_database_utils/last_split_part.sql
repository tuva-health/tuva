{#
    Returns the last element of string_text split on delimiter_text.
    Equivalent to split_part(..., -1) but works on Athena, whose adapter has
    no split_part override and so passes the negative index through to
    Trino verbatim (which rejects it: "Index must be greater than zero").
#}
{% macro last_split_part(string_text, delimiter_text) -%}
    {{ return(adapter.dispatch('last_split_part')(string_text, delimiter_text)) }}
{%- endmacro %}

{% macro default__last_split_part(string_text, delimiter_text) -%}
    {{ dbt.split_part(string_text, delimiter_text, -1) }}
{%- endmacro %}

{% macro athena__last_split_part(string_text, delimiter_text) -%}
    split_part(
        {{ string_text }},
        {{ delimiter_text }},
        cast(
            (length({{ string_text }}) - length(replace({{ string_text }}, {{ delimiter_text }}, ''))) / length({{ delimiter_text }}) + 1
            as integer
        )
    )
{%- endmacro %}

{% macro fabric__last_split_part(string_text, delimiter_text) -%}
    reverse(left(reverse({{ string_text }}), charindex({{ delimiter_text }}, reverse({{ string_text }})) - 1))
{%- endmacro %}
