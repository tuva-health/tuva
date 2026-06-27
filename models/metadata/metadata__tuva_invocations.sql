{{ config(
     schema = 'metadata',
     alias = 'tuva_invocations',
     materialized = 'table',
     tags = ['metadata']
   )
}}

select
    cast('{{ invocation_id }}' as {{ dbt.type_string() }}) as invocation_id
  , cast('the_tuva_project' as {{ dbt.type_string() }}) as package_name
  , cast('{{ the_tuva_project.get_tuva_package_version() }}' as {{ dbt.type_string() }}) as package_version
  , cast('{{ project_name }}' as {{ dbt.type_string() }}) as dbt_project_name
  , cast('{{ target.name }}' as {{ dbt.type_string() }}) as dbt_target_name
  , cast('{{ target.type }}' as {{ dbt.type_string() }}) as dbt_target_type
  , cast('{{ flags.WHICH }}' as {{ dbt.type_string() }}) as dbt_command
  , cast('{{ run_started_at }}' as {{ dbt.type_timestamp() }}) as run_started_at
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
