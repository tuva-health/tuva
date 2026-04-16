{% macro duckdb__load_seed(uri, pattern, compression, headers, null_marker) %}
{#
    No-op override for DuckDB in environments without S3/httpfs access.
    The seed table retains whatever data was loaded from the local CSV file.
#}
{% if execute %}
    {{ log("Skipping S3 seed load (no httpfs): s3://" ~ uri ~ "/" ~ pattern, info=True) }}
{% endif %}
{% endmacro %}
