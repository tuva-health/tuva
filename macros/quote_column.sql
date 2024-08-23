{% macro quote_column(column_name) %}
    {%- if target.type == 'fabric' -%}
        "{{ column_name }}"
    {%- else -%}
        {{ column_name }}
    {%- endif -%}
{% endmacro %}
