{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with diagnosis as (
    select distinct
        c.encounter_id
      , c.data_source
    from {{ ref('ahrq_measures__stg_pqi_condition') }} as c
    inner join {{ ref('pqi__value_sets') }} as pqi
      on c.normalized_code = pqi.code
      and c.normalized_code_type = 'icd-10-cm'
      and pqi.value_set_name = 'diabetes_diagnosis_codes'
      and pqi_number = '16'
    where c.encounter_id is not null
)


, procedures as (
    select distinct
        p.encounter_id
      , p.data_source
    from {{ ref('ahrq_measures__stg_pqi_procedure') }} as p
    inner join diagnosis as d
      on p.encounter_id = d.encounter_id
      and d.data_source = p.data_source
    inner join {{ ref('pqi__value_sets') }} as pqi
      on p.normalized_code = pqi.code
      and p.normalized_code_type = 'icd-10-pcs'
      and pqi.value_set_name = 'lower-extremity_amputation_procedure_codes'
      and pqi_number = '16'
)

select
    e.data_source
  , e.person_id
  , e.year_number
  , e.encounter_id
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }} as e
inner join {{ ref('ahrq_measures__int_pqi_16_denom') }} as denom
  on e.person_id = denom.person_id
  and e.data_source = denom.data_source
  and e.year_number = denom.year_number
    inner join procedures as p
  on e.encounter_id = p.encounter_id
  and e.data_source = p.data_source
left outer join {{ ref('ahrq_measures__int_pqi_16_exclusions') }} as shared
  on e.encounter_id = shared.encounter_id
  and e.data_source = shared.data_source
where shared.encounter_id is null
