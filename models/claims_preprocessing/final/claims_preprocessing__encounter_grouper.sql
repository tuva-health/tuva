{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_marts_enabled',True))
   )
}}


-- *************************************************
-- This dbt model assigns service categories to
-- every claim line in the medical_claim table.
-- It returns a table with these 4 columns:
--      claim_id
--      claim_line_number
--      service_category_1
--      service_category_2
-- *************************************************



select
  aa.claim_id,
  aa.claim_line_number,
  aa.claim_type,
  aa.patient_id,
  aa.member_id,
-- Add service categories:
  cc.service_category_1,
  cc.service_category_2,  
-- Fields with encounter data to append to medical_claim:
--   bb.start_date,
--   bb.end_date,
  bb.encounter_type,
  bb.encounter_id,
  bb.encounter_start_date,
  bb.encounter_end_date,
  bb.encounter_admit_source_code,
  bb.encounter_admit_type_code,
  bb.encounter_discharge_disposition_code,
  bb.orphan_claim_flag,
  bb.encounter_count,
  aa.data_source
from {{ ref('input_layer__medical_claim') }} aa

left join
{{ ref('claims_preprocessing__encounter_data_for_medical_claims') }} bb
on aa.claim_id = bb.claim_id
and aa.patient_id = bb.patient_id

left join
{{ ref('claims_preprocessing__service_categories') }} cc
on aa.claim_id = cc.claim_id
and aa.claim_line_number = cc.claim_line_number



