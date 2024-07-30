
{# This macro is intended for use by dbt-invoke #}
{% macro _log_columns_list(sql=none, resource_name=none) %}
    {% if sql is none %}
        {% set sql = 'select * from ' ~ ref(resource_name) %}
    {% endif %}
    {% if execute %}
        {{ log(get_columns_in_query(sql), info=True) }}
    {% endif %}
{% endmacro %}
