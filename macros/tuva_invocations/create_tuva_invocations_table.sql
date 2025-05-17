{% macro create_tuva_invocations_table() %}
  
  {%- set schema_name = var('tuva_schema_prefix') ~ '_metadata' if var('tuva_schema_prefix',None) != None else 'metadata' -%}
  
  {% do adapter.create_schema(api.Relation.create(database=target.database, schema=schema_name)) %}
    
  {%- set table_name = 'tuva_invocations' -%}
  {%- set target_type = target.type -%}
  
  {%- if target_type == 'bigquery' or target_type == 'fabric' -%}
    {%- set sql -%}
        CREATE OR REPLACE TABLE {{ schema_name }}.{{ table_name }}
        (
            invocation_id {{ dbt.type_string() }},
            project_name {{ dbt.type_string() }},
            tuva_package_version {{ dbt.type_string() }},
            run_started_at {{ dbt.type_timestamp() }}
        )
    {%- endset -%}
  {%- elif target_type == 'snowflake' -%}
    {%- set sql -%}
        CREATE OR REPLACE TABLE {{ schema_name }}.{{ table_name }} 
        (
            invocation_id {{ dbt.type_string() }},
            project_name {{ dbt.type_string() }},
            tuva_package_version {{ dbt.type_string() }},
            run_started_at {{ dbt.type_timestamp() }}
        )
    {%- endset -%}
  {%- elif target_type == 'redshift' or target_type == 'postgres' or target_type == 'duckdb' -%}
    {%- set sql -%}
        CREATE TABLE IF NOT EXISTS {{ schema_name }}.{{ table_name }} 
        (
            invocation_id {{ dbt.type_string() }},
            project_name {{ dbt.type_string() }},
            tuva_package_version {{ dbt.type_string() }},
            run_started_at {{ dbt.type_timestamp() }}
        )
    {%- endset -%}
  {%- else -%}
    {{ exceptions.raise_compiler_error("Unsupported target platform: " ~ target.type) }}
  {%- endif -%}

  {% do run_query(sql) %}
{% endmacro %}