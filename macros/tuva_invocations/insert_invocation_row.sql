{% macro log_invocation_start() %}
    {# Get the schema name using the same logic as in create_tuva_invocations_table #}
    {%- set schema_name = var('tuva_schema_prefix') ~ '_metadata' if var('tuva_schema_prefix',None) != None else 'metadata' -%}
    
    {# Capture the boolean result from create_tuva_invocations_table #}
    {%- set table_created = the_tuva_project.create_tuva_invocations_table() -%}
    
    {# Only run DML if table_created is true. It will be false if a non-supported database is used. #}
    {%- if table_created -%}
        {# Insert the record #}
        {% set query %}
            insert into {{ schema_name }}.tuva_invocations
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

        {%- do the_tuva_project.trim_tuva_invocations() -%}
    {%- endif -%}

{% endmacro %}