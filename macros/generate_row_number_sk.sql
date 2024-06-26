{% macro generate_row_number_sk(order_by_columns) %}
    ROW_NUMBER() OVER (ORDER BY {{ order_by_columns }}) AS surrogate_key
{% endmacro %}
