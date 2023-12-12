{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select
  inst.patient_id,
  inst.claim_id,
  med.claim_line_number,
  inst.encounter_id,
  '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('emergency_department__int_institutional_encounter_id') }} inst
left join {{ ref('emergency_department__stg_medical_claim') }} med
    on inst.claim_id = med.claim_id

union distinct

select
  patient_id,
  claim_id,
  claim_line_number,
  encounter_id,
  '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('emergency_department__int_professional_encounter_id') }}
where (orphan_claim_flag = 0) and (encounter_count = 1)
