{% macro zero_pad(column, length) %}
{#
    Left-pads a code with zeros to a fixed width, for joining against
    terminology seeds that store codes zero-padded.

    Every adapter this package targets has lpad() except Fabric, whose T-SQL
    has no such function; there the idiom is RIGHT(REPLICATE('0', n) + col, n).

        on {{ zero_pad('med.place_of_service_code', 2) }} = pos.place_of_service_code
#}
{%- if target.type == 'fabric' -%}
RIGHT(REPLICATE('0', {{ length }}) + {{ column }}, {{ length }})
{%- else -%}
lpad({{ column }}, {{ length }}, '0')
{%- endif -%}
{% endmacro %}
