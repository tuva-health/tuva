{% macro quote_column(column_name) %}
    {%- if target.type in ('fabric', 'sqlserver') -%}
        [{{ column_name }}]
    {%- else -%}
        {{ column_name }}
    {%- endif -%}
{% endmacro %}
