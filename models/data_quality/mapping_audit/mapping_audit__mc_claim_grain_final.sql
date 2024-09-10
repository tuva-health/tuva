{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}




select
  claim_line_number.claim_id,
  claim_line_number.claim_line_number_unique,

  claim_type.claim_type_always_populated,
  claim_type.claim_type_unique,
  claim_type.claim_type_valid_values,

  member_id.member_id_always_populated,
  member_id.member_id_unique,

  plan.plan_always_populated,
  plan.plan_unique,

--  claim_start_date.claim_start_date_always_populated,
  claim_start_date.claim_start_date_unique,

--  claim_end_date.claim_end_date_always_populated,
  claim_end_date.claim_end_date_unique,

  claim_line_start_date.claim_line_start_date_always_populated,

  claim_line_end_date.claim_line_end_date_always_populated,

  admission_date.admission_date_unique,

  discharge_date.discharge_date_unique,

  discharge_disposition_code.discharge_disposition_code_correct_length,
  discharge_disposition_code.discharge_disposition_code_unique,

  place_of_service_code.place_of_service_code_correct_length,

  bill_type_code.bill_type_code_correct_length,
  bill_type_code.bill_type_code_unique,

  ms_drg_code.ms_drg_code_correct_length,
  ms_drg_code.ms_drg_code_unique,

  apr_drg_code.apr_drg_code_correct_length,
  apr_drg_code.apr_drg_code_unique,

  revenue_center_code.revenue_center_code_correct_length,

  diagnosis_code_type.diagnosis_code_type_needed,
  diagnosis_code_type.diagnosis_code_type_valid,
  diagnosis_code_type.diagnosis_code_type_unique,

  diagnosis_code.diagnosis_code_unique,

  procedure_code_type.procedure_code_type_needed,
  procedure_code_type.procedure_code_type_valid,
  procedure_code_type.procedure_code_type_unique,

  procedure_code.procedure_code_unique,

  data_source.data_source_always_populated,
  data_source.data_source_unique
  
from {{ ref('mapping_audit__mc_claim_line_number') }} claim_line_number

left join {{ ref('mapping_audit__mc_claim_type') }} claim_type
on claim_line_number.claim_id = claim_type.claim_id

left join {{ ref('mapping_audit__mc_member_id') }} member_id
on claim_line_number.claim_id = member_id.claim_id

left join {{ ref('mapping_audit__mc_plan') }} plan
on claim_line_number.claim_id = plan.claim_id

left join {{ ref('mapping_audit__mc_claim_start_date') }} claim_start_date
on claim_line_number.claim_id = claim_start_date.claim_id

left join {{ ref('mapping_audit__mc_claim_end_date') }} claim_end_date
on claim_line_number.claim_id = claim_end_date.claim_id

left join {{ ref('mapping_audit__mc_claim_line_start_date') }} claim_line_start_date
on claim_line_number.claim_id = claim_line_start_date.claim_id

left join {{ ref('mapping_audit__mc_claim_line_end_date') }} claim_line_end_date
on claim_line_number.claim_id = claim_line_end_date.claim_id

left join {{ ref('mapping_audit__mc_admission_date') }} admission_date
on claim_line_number.claim_id = admission_date.claim_id

left join {{ ref('mapping_audit__mc_discharge_date') }} discharge_date
on claim_line_number.claim_id = discharge_date.claim_id

left join {{ ref('mapping_audit__mc_discharge_disposition_code') }} discharge_disposition_code
on claim_line_number.claim_id = discharge_disposition_code.claim_id

left join {{ ref('mapping_audit__mc_place_of_service_code') }} place_of_service_code
on claim_line_number.claim_id = place_of_service_code.claim_id

left join {{ ref('mapping_audit__mc_bill_type_code') }} bill_type_code
on claim_line_number.claim_id = bill_type_code.claim_id

left join {{ ref('mapping_audit__mc_ms_drg_code') }} ms_drg_code
on claim_line_number.claim_id = ms_drg_code.claim_id

left join {{ ref('mapping_audit__mc_apr_drg_code') }} apr_drg_code
on claim_line_number.claim_id = apr_drg_code.claim_id

left join {{ ref('mapping_audit__mc_revenue_center_code') }} revenue_center_code
on claim_line_number.claim_id = revenue_center_code.claim_id

left join {{ ref('mapping_audit__mc_diagnosis_code_type') }} diagnosis_code_type
on claim_line_number.claim_id = diagnosis_code_type.claim_id

left join {{ ref('mapping_audit__mc_diagnosis_code') }} diagnosis_code
on claim_line_number.claim_id = diagnosis_code.claim_id

left join {{ ref('mapping_audit__mc_procedure_code_type') }} procedure_code_type
on claim_line_number.claim_id = procedure_code_type.claim_id

left join {{ ref('mapping_audit__mc_procedure_code') }} procedure_code
on claim_line_number.claim_id = procedure_code.claim_id

left join {{ ref('mapping_audit__mc_data_source') }} data_source
on claim_line_number.claim_id = data_source.claim_id
