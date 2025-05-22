

{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


select
  aa.claim_id,
  aa.data_source,
  aa.recorded_date,
  bb.person_id,
  bb.member_id,
  bb.diagnosis_code_type,
  bb.diagnosis_code_1,
  bb.diagnosis_code_2,
  bb.diagnosis_code_3,
  bb.diagnosis_code_4,
  bb.diagnosis_code_5,
  bb.diagnosis_code_6,
  bb.diagnosis_code_7,
  bb.diagnosis_code_8,
  bb.diagnosis_code_9,
  bb.diagnosis_code_10,
  bb.diagnosis_code_11,
  bb.diagnosis_code_12,
  bb.diagnosis_code_13,
  bb.diagnosis_code_14,
  bb.diagnosis_code_15,
  bb.diagnosis_code_16,
  bb.diagnosis_code_17,
  bb.diagnosis_code_18,
  bb.diagnosis_code_19,
  bb.diagnosis_code_20,
  bb.diagnosis_code_21,
  bb.diagnosis_code_22,
  bb.diagnosis_code_23,
  bb.diagnosis_code_24,
  bb.diagnosis_code_25,
  bb.diagnosis_poa_1,
  bb.diagnosis_poa_2,
  bb.diagnosis_poa_3,
  bb.diagnosis_poa_4,
  bb.diagnosis_poa_5,
  bb.diagnosis_poa_6,
  bb.diagnosis_poa_7,
  bb.diagnosis_poa_8,
  bb.diagnosis_poa_9,
  bb.diagnosis_poa_10,
  bb.diagnosis_poa_11,
  bb.diagnosis_poa_12,
  bb.diagnosis_poa_13,
  bb.diagnosis_poa_14,
  bb.diagnosis_poa_15,
  bb.diagnosis_poa_16,
  bb.diagnosis_poa_17,
  bb.diagnosis_poa_18,
  bb.diagnosis_poa_19,
  bb.diagnosis_poa_20,
  bb.diagnosis_poa_21,
  bb.diagnosis_poa_22,
  bb.diagnosis_poa_23,
  bb.diagnosis_poa_24,
  bb.diagnosis_poa_25
from {{ ref('core__diagnosis_recorded_date') }} aa 
left join {{ ref('core__dedupe_header_fields') }} bb 
on aa.claim_id = bb.claim_id
and aa.data_source = bb.data_source
