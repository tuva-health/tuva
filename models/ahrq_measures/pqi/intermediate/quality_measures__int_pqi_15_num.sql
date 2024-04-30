select
    e.data_source
  , e.patient_id
  , e.year_number
  , e.encounter_id
from {{ ref('quality_measures__stg_pqi_inpatient_encounter') }} as e
inner join {{ ref('pqi__value_sets') }} as hyp
  on e.primary_diagnosis_code = hyp.code
  and hyp.value_set_name = 'asthma_diagnosis_codes'
  and hyp.pqi_number = '15'
inner join {{ ref('quality_measures__int_pqi_15_denom') }} as denom
  on e.patient_id = denom.patient_id
  and e.data_source = denom.data_source
  and e.year_number = denom.year_number
left join {{ ref('quality_measures__int_pqi_15_exclusions') }} as shared
  on e.encounter_id = shared.encounter_id
  and e.data_source = shared.data_source
where shared.encounter_id is null
