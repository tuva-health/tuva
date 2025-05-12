{% test warn_if_null_percentage_above_zero(model, column_name) %}

{#-
    Calculates the percentage of NULL values in a column.
    Passes if the percentage is 0%.
    Fails (or Warns based on severity config) if the percentage is > 0%.
    Includes the column name, error description, and a query to find NULL rows in the output.

    Args:
        model: The model object (implicitly passed by dbt).
        column_name: The name of the column to test (implicitly passed by dbt).
-#}

{% set table_name = model.alias if model.alias else model.name %}
{% set query_to_find_nulls = "SELECT * FROM " ~ model ~ " WHERE " ~ column_name ~ " IS NULL;" %}

WITH validation AS (

    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END) AS null_rows
    FROM {{ model }}

),

validation_errors AS (

    SELECT
        total_rows,
        null_rows,
        CASE
            WHEN total_rows = 0 THEN 0.0 -- Avoid division by zero for empty tables
            -- Cast to FLOAT for cross-db compatibility in division
            ELSE (CAST(null_rows AS FLOAT) * 100.0) / total_rows
        END AS null_percentage
    FROM validation
    -- Only proceed if there are NULL rows to report
    WHERE null_rows > 0

)

SELECT
    '{{ column_name }}' AS column_tested,
    'Column `{{ column_name }}` in model `{{ table_name }}` has ' || printf('%.2f', null_percentage) || '% NULL values (' || CAST(null_rows AS VARCHAR) || '/' || CAST(total_rows AS VARCHAR) || ' rows).' AS error_description,
    '{{ query_to_find_nulls | escape }}' AS query_to_find_nulls,
    null_percentage -- Keep the percentage for potential threshold checks later
FROM validation_errors
-- The test fails (returns rows) if the validation_errors CTE has any rows (i.e., null_rows > 0).

{% endtest %}
