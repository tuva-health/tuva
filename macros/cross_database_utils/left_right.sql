{% macro left_chars(column, length) %}
{#
    The first n characters of a string.

    Every adapter this package targets has left() except Athena, whose Trino
    engine has no such function; there the idiom is substr(col, 1, n).

        , cast({{ left_chars('year_month', 4) }} as {{ dbt.type_int() }}) as performance_year
#}
{%- if target.type == 'athena' -%}
substr({{ column }}, 1, {{ length }})
{%- else -%}
left({{ column }}, {{ length }})
{%- endif -%}
{% endmacro %}


{% macro right_chars(column, length) %}
{#
    The last n characters of a string. Athena spells it with a negative start
    offset rather than right().

        on {{ right_chars("concat('00', trim(x.code))", 2) }} = ...
#}
{%- if target.type == 'athena' -%}
substr({{ column }}, -{{ length }})
{%- else -%}
right({{ column }}, {{ length }})
{%- endif -%}
{% endmacro %}
