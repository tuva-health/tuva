{% macro to_char(date_column, format_string) %}
    {% if target.type == 'bigquery' %}
        {% if format_string == 'YYYYMM' %}
            FORMAT_DATE('%Y%m', CAST({{ date_column }} AS DATE))
        {% else %}
            {{ exceptions.raise_compiler_error("Unsupported format for BigQuery: " ~ format_string) }}
        {% endif %}
    {% elif target.type == 'fabric' %}
        {% if format_string == 'YYYYMM' %}
            FORMAT(CAST({{ date_column }} AS DATE), 'yyyyMM')
        {% else %}
            {{ exceptions.raise_compiler_error("Unsupported format for Fabric: " ~ format_string) }}
        {% endif %}
    {% elif target.type == 'duckdb' %}
        {% if format_string == 'YYYYMM' %}
            STRFTIME({{ date_column }}, '%Y%m')
        {% else %}
            {{ exceptions.raise_compiler_error("Unsupported format for DuckDB: " ~ format_string) }}
        {% endif %}
    {% elif target.type == 'databricks' %}
        {% if format_string == 'YYYYMM' %}
            DATE_FORMAT({{ date_column }}, 'yyyyMM')
        {% else %}
            {{ exceptions.raise_compiler_error("Unsupported format for Databricks: " ~ format_string) }}
        {% endif %}
    {% else %}
        {# Default for Redshift, Snowflake, Postgres, etc. that support TO_CHAR #}
        TO_CHAR({{ date_column }}, '{{ format_string }}')
    {% endif %}
{% endmacro %}

