{% test not_negative(model, column_name) %}

{#
    Generic test: asserts that no numeric values in the column
    are negative (less than zero).

    Usage in schema.yml:
      tests:
        - not_negative
#}

select
    {{ column_name }} as failing_value
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
