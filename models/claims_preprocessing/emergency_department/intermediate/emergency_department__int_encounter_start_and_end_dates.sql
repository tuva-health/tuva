{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


with add_encounter_id_to_emergency_department_encounters as (
select
  aip.claim_id as claim_id,
  aip.patient_id as patient_id,
  aip.start_date as start_date,
  aip.end_date as end_date,
  eid.encounter_id as encounter_id
from {{ ref('emergency_department__int_institutional_claims') }} aip
left join {{ ref('emergency_department__int_institutional_encounter_id') }} eid
  on aip.patient_id = eid.patient_id
  and aip.claim_id = eid.claim_id
),

encounter_start_and_end_dates as (
select
  patient_id,
  encounter_id,
  min(start_date) as encounter_start_date,
  max(end_date) as encounter_end_date
from add_encounter_id_to_emergency_department_encounters
group by patient_id, encounter_id
)

select 
    patient_id
    , encounter_id
    , encounter_start_date
    , encounter_end_date
    , coalesce(encounter_start_date, encounter_end_date) as determined_encounter_start_date
    , coalesce(encounter_end_date, encounter_start_date) as determined_encounter_end_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from encounter_start_and_end_dates
