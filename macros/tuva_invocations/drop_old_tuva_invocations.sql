{% macro drop_old_tuva_invocations() %}
    {%- set schema_name = var('tuva_schema_prefix') ~ '_metadata' if var('tuva_schema_prefix',None) != None else 'metadata' -%}
    {% set query %}
        delete from {{ schema_name }}.tuva_invocations
        where run_started_at < cast({{ dateadd('month', -1, current_timestamp()) }} as {{ dbt.type_timestamp() }})
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}
