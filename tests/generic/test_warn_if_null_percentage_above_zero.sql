{# tests/generic/test_warn_if_null_percentage_above_zero.sql #}
{% test warn_if_null_percentage_above_zero(model, column_name) %}

{#-
    Calculates the percentage of NULL values in a column using ANSI SQL.
    Passes if the percentage is 0%.
    Warns if the percentage is between 0% and 100%.
    Includes the column name, error description (with formatted percentage),
    and a query to find NULL rows in the output.

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

calculation AS (

    SELECT
        total_rows,
        null_rows,
        CASE
            WHEN total_rows = 0 THEN 0.0 -- Avoid division by zero for empty tables
            -- Cast to DOUBLE PRECISION for division
            ELSE (CAST(null_rows AS DOUBLE PRECISION) * 100.0) / total_rows
        END AS null_percentage_raw
    FROM validation
    WHERE null_rows > 0 -- Only calculate if there are nulls to report

),

-- Format the percentage using standard ROUND and CAST to DECIMAL for consistent .XX format
validation_errors AS (
    SELECT
        total_rows,
        null_rows,
        null_percentage_raw,
        -- Round to 2 decimal places, cast to DECIMAL(5,2) to enforce scale, then to VARCHAR
        -- DECIMAL(5,2) is suitable for values 0.00 to 100.00
        CAST(CAST(ROUND(null_percentage_raw, 2) AS DECIMAL(5, 2)) AS VARCHAR) AS null_percentage_formatted
    FROM calculation
)

SELECT
    '{{ column_name }}' AS column_tested,
    -- Use ANSI SQL string concatenation '||'
    'Column `{{ column_name }}` in model `{{ table_name }}` has ' || null_percentage_formatted || '% NULL values (' || CAST(null_rows AS VARCHAR) || '/' || CAST(total_rows AS VARCHAR) || ' rows).' AS error_description,
    '{{ query_to_find_nulls }}' AS query_to_find_nulls
FROM validation_errors
-- The test fails (returns rows) if the validation_errors CTE has any rows (i.e., null_rows > 0).

{% endtest %}
