{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select
    encounter_id
  , data_source
  , exclusion_reason
  , row_number() over (
      partition by encounter_id, data_source
      order by exclusion_reason
    ) as exclusion_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_shared_exclusion_union') }}
