{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

/* Exclude patients with missing age */
select
    data_source
    , person_id
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_patient') }}
where birth_date is null
