{% macro create_tuva_invocations_table(schema_name) %}
    {# Creates a tuva invocations table. Returns true if the table is created or exists. Returns false if a non-supported platform is used. #}
    {% do adapter.create_schema(api.Relation.create(database=target.database, schema=schema_name)) %}

    {%- set table_name = 'tuva_invocations' -%}
    {%- set target_type = target.type -%}
  
    {%- if target_type in ['redshift','postgres','duckdb'] -%}
        {%- set sql -%}
            CREATE TABLE IF NOT EXISTS {{ schema_name }}.{{ table_name }}
            (
                invocation_id {{ dbt.type_string() }},
                project_name {{ dbt.type_string() }},
                tuva_package_version {{ dbt.type_string() }},
                run_started_at {{ dbt.type_timestamp() }}
            )
        {%- endset -%}
        {% do run_query(sql) %}
        {{ return(true) }}
    {%- elif target_type in ['snowflake','databricks','bigquery'] -%}
        {%- set sql -%}
            CREATE OR REPLACE TABLE {{ schema_name }}.{{ table_name }}
            (
                invocation_id {{ dbt.type_string() }},
                project_name {{ dbt.type_string() }},
                tuva_package_version {{ dbt.type_string() }},
                run_started_at {{ dbt.type_timestamp() }}
            )
        {%- endset -%}
        {% do run_query(sql) %}
        {{ return(true) }}
    {%- elif target_type in ['fabric'] -%}
        {%- set sql -%}
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{{ table_name }}' AND TABLE_SCHEMA = '{{ schema_name }}')
            CREATE TABLE {{ schema_name }}.{{ table_name }}
            (
                invocation_id {{ dbt.type_string() }},
                project_name {{ dbt.type_string() }},
                tuva_package_version {{ dbt.type_string() }},
                run_started_at {{ dbt.type_timestamp() }}
            )
        {%- endset -%}
        {% do run_query(sql) %}
        {{ return(true) }}
    {%- else -%}
        {{ return(false) }}
    {%- endif -%}

{% endmacro %}


{% macro log_invocation_start() %}
    {# Check if Tuva resources are selected #}
    {% if not the_tuva_project.is_tuva_selected() %}
        {% do log("No Tuva resources selected, skipping invocation tracking", info=true) %}
        {% do return('') %}
    {% endif %}

    {# Check if invocation tracking is disabled #}
    {% if the_tuva_project.get_config_var('disable_tuva_invocation_tracking') %}
        {% do log("Tuva invocation tracking disabled via disable_tuva_invocation_tracking variable", info=true) %}
        {% do return('') %}
    {% endif %}

    {# Records the invocation start with the tuva project package version. #}
    {%- set schema_name = generate_schema_name(custom_schema_name='metadata') %}
    {# Capture the boolean result from create_tuva_invocations_table #}
    {%- set table_created = the_tuva_project.create_tuva_invocations_table(schema_name) -%}

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

        {%- do the_tuva_project.drop_old_tuva_invocations(schema_name) -%}

    {%- endif -%}

{% endmacro %}


{% macro drop_old_tuva_invocations(schema_name) %}
    {# Deletes tuva invocation records older than the retention period. #}
    {%- set retention_days = var('tuva_metadata_retention_days', 30) | int -%}
    
    {% set dateadd = adapter.dispatch('dateadd', 'datetime') %}
    {% set query %}
        delete from {{ schema_name }}.tuva_invocations
        where run_started_at < cast({{ dateadd('day', -1 * retention_days, current_timestamp()) }} as {{ dbt.type_timestamp() }})
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}
