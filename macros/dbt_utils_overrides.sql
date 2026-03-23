{% macro fabric__test_expression_is_true(model, expression, column_name) %}

{% set failure_column = 'expression_is_true_failure' %}
{% set column_list = '*' if should_store_failures() else "1 as " ~ failure_column %}

select
    {{ column_list }}
from {{ model }}
{% if column_name is none %}
where not ({{ expression }})
{%- else %}
where not ({{ column_name }} {{ expression }})
{%- endif %}

{% endmacro %}
