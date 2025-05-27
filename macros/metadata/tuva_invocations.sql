{% macro create_tuva_invocations_table() %}
  
  {%- set schema_name = var('tuva_schema_prefix') ~ '_metadata' if var('tuva_schema_prefix',None) != None else 'metadata' -%}
  
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
    {%- if target_type in ['fabric'] -%}
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
    {%- endif -%}
  {%- else -%}
    {{ return(false) }}
  {%- endif -%}

{% endmacro %}


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

        {%- do the_tuva_project.drop_old_tuva_invocations() -%}
    {%- endif -%}

{% endmacro %}


{% macro drop_old_tuva_invocations() %}
    {%- set schema_name = var('tuva_schema_prefix') ~ '_metadata' if var('tuva_schema_prefix',None) != None else 'metadata' -%}
    {%- set retention_days = var('tuva_metadata_retention_days', 30) -%}
    {%- set retention_days = retention_days | int -%}
    
    {# Delete records older than the retention period #}
    {% do log("Deleting Tuva invocations older than " ~ retention_days ~ " days", info=True) %}
    
    {# Use the dateadd function to calculate the cutoff date #}
     {% set dateadd = adapter.dispatch('dateadd', 'datetime') %}
    {% set query %}
        delete from {{ schema_name }}.tuva_invocations
        where run_started_at < cast({{ dateadd('day', -1 * retention_days, current_timestamp()) }} as {{ dbt.type_timestamp() }})
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}
