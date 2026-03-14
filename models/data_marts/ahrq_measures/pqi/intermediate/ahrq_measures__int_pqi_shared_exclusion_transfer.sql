{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

-- Exclude transfers from hospital, SNF, or other healthcare facility
select
    encounter_id
  , data_source
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }}
where
  admit_source_code in ('4', '5', '6')
