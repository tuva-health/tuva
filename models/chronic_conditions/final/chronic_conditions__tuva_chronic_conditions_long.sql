{{ config(
     enabled = var('tuva_chronic_conditions_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with all_conditions as (
select
  person_id
  , normalized_code
  , recorded_date
    from {{ ref('tuva_chronic_conditions__stg_core__condition') }}
)


, conditions_with_first_and_last_diagnosis_date as (
select
  person_id
  , normalized_code as icd_10_cm
  , min(recorded_date) as first_diagnosis_date
  , max(recorded_date) as last_diagnosis_date
from all_conditions
group by person_id, normalized_code

)


select
  aa.person_id
  , bb.concept_name as condition
  , min(first_diagnosis_date) as first_diagnosis_date
  , max(last_diagnosis_date) as last_diagnosis_date
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from conditions_with_first_and_last_diagnosis_date as aa
inner join {{ ref('clinical_concept_library__value_set_member_relevant_fields') }} as bb
on aa.icd_10_cm = bb.code
group by aa.person_id, bb.concept_name
