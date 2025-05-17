-- This model is used to create a table with the invocation metadata for the Tuva package.
-- This is model is for dbt metadata only. Data is populated via macros.
{{ config(
    materialized='incremental',
    schema='metadata',
    alias='tuva_invocations',
    enabled= false
) }}
select 
    cast(null as {{ dbt.type_string() }}) as invocation_id,
    cast(null as {{ dbt.type_string() }}) as project_name,
    cast(null as {{ dbt.type_string() }}) as tuva_package_version,
    cast(null as {{ dbt.type_string() }}) as run_started_at
where 0 = 1
