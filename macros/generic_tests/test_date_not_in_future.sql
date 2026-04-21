{% test date_not_in_future(model, column_name) %}

{#
    Generic test: asserts that no date values in the column
    are greater than the current date.

    Usage in schema.yml:
      tests:
        - date_not_in_future
#}

select
    {{ column_name }} as failing_date
from {{ model }}
where {{ column_name }} > current_date

{% endtest %}
