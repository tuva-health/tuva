{% macro create_tuva_invocations_table() %}
  
  {%- set schema_name = var('tuva_schema_prefix') ~ '_metadata' if var('tuva_schema_prefix',None) != None else 'metadata' -%}
  
  {% do adapter.create_schema(api.Relation.create(database=target.database, schema=schema_name)) %}
    
  {%- set table_name = 'tuva_invocations' -%}
  {%- set target_type = target.type -%}
  
  {%- if target_type in ['bigquery','redshift','postgres','duckdb'] -%}
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
  {%- elif target_type in ['snowflake','databricks'] -%}
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