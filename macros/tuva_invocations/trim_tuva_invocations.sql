{% macro trim_tuva_invocations() %}
    {% set query %}
        delete from metadata.tuva_invocations
        where run_started_at < ({{ current_timestamp() }} - interval '7 days')
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}
