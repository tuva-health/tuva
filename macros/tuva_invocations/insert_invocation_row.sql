{% macro log_invocation_start() %}
    {# Ensure that the schema and table exist first #}
    {% do the_tuva_project.create_tuva_invocations_table() %}
    {% do the_tuva_project.trim_tuva_invocations() %}

    {# Insert the record #}
    {% set query %}
        insert into metadata.tuva_invocations
        (
            invocation_id,
            project_name,
            tuva_package_version,
            run_started_at
        )
        values
        ( 
            '{{ invocation_id }}',
            '{{ project_name }}',
            '{{ the_tuva_project.get_tuva_package_version() }}',
            '{{ run_started_at }}'
        )
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}
