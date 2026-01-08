{% test warn_if_null_percentage_above_zero(model, column_name) %}

{#-
    Calculates the percentage of NULL values in a column using ANSI SQL.
    Passes if the percentage is < 100%.
    Warns if the percentage is 100%.
    Includes the column name, error description (with formatted percentage),
    and a query to find NULL rows in the output.

    Args:
        model: The model object (implicitly passed by dbt).
        column_name: The name of the column to test (implicitly passed by dbt).
-#}

{% set table_name = model.alias if model.alias else model.name %}
{% set query_to_find_nulls = "SELECT * FROM " ~ model ~ " WHERE " ~ adapter.quote(column_name) ~ " IS NULL;" %}

WITH validation AS (

    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN {% if is_fabric() %}{{ adapter.quote(column_name) }}{% else %}{{ column_name }}{% endif %} IS NULL THEN 1 ELSE 0 END) AS null_rows
    FROM {{ model }}

),

calculation AS (

    SELECT
        total_rows,
        null_rows,
        CASE
            WHEN total_rows = 0 THEN 0.0
            ELSE
                {% if target.type == 'bigquery' %}
                    (CAST(null_rows AS FLOAT64) * 100.0) / total_rows
                {% elif target.type == 'fabric' %}
                    (CAST(null_rows AS FLOAT) * 100.0) / total_rows
                {% elif target.type in ('databricks', 'duckdb', 'athena') %}
                    (CAST(null_rows AS DOUBLE) * 100.0) / total_rows
                {% else %}
                    (CAST(null_rows AS DOUBLE PRECISION) * 100.0) / total_rows
                {% endif %}
        END AS null_percentage_raw
    FROM validation
    WHERE total_rows = null_rows

),

validation_errors AS (
    SELECT
        total_rows,
        null_rows,
        null_percentage_raw,
        {% if target.type == 'bigquery' %}
            CAST(ROUND(null_percentage_raw, 2) AS STRING) AS null_percentage_formatted
        {% elif target.type == 'fabric' %}
            CAST(CAST(ROUND(null_percentage_raw, 2) AS DECIMAL(5, 2)) AS {{ varchar() }}) AS null_percentage_formatted
        {% elif target.type in ('databricks', 'duckdb', 'athena') %}
            CAST(CAST(ROUND(null_percentage_raw, 2) AS DECIMAL(5, 2)) AS STRING) AS null_percentage_formatted
        {% else %}
            CAST(CAST(ROUND(null_percentage_raw, 2) AS DECIMAL(5, 2)) AS {{ varchar() }}) AS null_percentage_formatted
        {% endif %}
    FROM calculation
)

SELECT
    '{{ column_name }}' AS column_tested,
    {% if target.type == 'bigquery' %}
        CONCAT('Column `{{ column_name }}` in model `{{ table_name }}` has ', null_percentage_formatted, '% NULL values (', CAST(null_rows AS STRING), '/', CAST(total_rows AS STRING), ' rows).') AS error_description,
    {% elif target.type == 'fabric' %}
        CONCAT('Column `{{ column_name }}` in model `{{ table_name }}` has ', null_percentage_formatted, '% NULL values (', CAST(null_rows AS {{ varchar() }}), '/', CAST(total_rows AS {{ varchar() }}), ' rows).') AS error_description,
    {% elif target.type in ('databricks', 'duckdb', 'athena', 'snowflake', 'redshift') %}
        'Column `{{ column_name }}` in model `{{ table_name }}` has ' || null_percentage_formatted || '% NULL values (' || CAST(null_rows AS {{ varchar() }}) || '/' || CAST(total_rows AS {{ varchar() }}) || ' rows).' AS error_description,
    {% else %}
        'Column `{{ column_name }}` in model `{{ table_name }}` has ' || null_percentage_formatted || '% NULL values (' || CAST(null_rows AS {{ varchar() }}) || '/' || CAST(total_rows AS {{ varchar() }}) || ' rows).' AS error_description,
    {% endif %}
    '{{ query_to_find_nulls }}' AS query_to_find_nulls
FROM validation_errors
{% endtest %}
