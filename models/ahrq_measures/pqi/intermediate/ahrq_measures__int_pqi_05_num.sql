{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select
    e.data_source
  , e.person_id
  , e.year_number
  , e.encounter_id
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }} as e
inner join {{ ref('ahrq_measures__int_pqi_05_denom') }} as denom
  on e.person_id = denom.person_id
  and e.data_source = denom.data_source
  and e.year_number = denom.year_number
left outer join {{ ref('pqi__value_sets') }} as copd
  on e.primary_diagnosis_code = copd.code
  and copd.value_set_name = 'chronic_obstructive_pulmonary_disorder'
  and copd.pqi_number = '05'
left outer join {{ ref('pqi__value_sets') }} as asthma
  on e.primary_diagnosis_code = asthma.code
  and asthma.value_set_name = 'asthma'
  and asthma.pqi_number = '05'
left outer join {{ ref('ahrq_measures__int_pqi_05_exclusions') }} as shared
  on e.encounter_id = shared.encounter_id
  and e.data_source = shared.data_source
where shared.encounter_id is null
  and (
    asthma.code is not null
    or copd.code is not null
    )
