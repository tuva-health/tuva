{{ config(
     enabled = var('core_enabled',var('tuva_marts_enabled',True))
   )
}}

-- *************************************************
-- This dbt model creates the medical_claim table
-- in core. It adds these 4 fields to the input layer
-- medical claim table:
--      encounter_type
--      encounter_id
--      service_category_1
--      service_category_2
-- *************************************************




with select_relevant_columns_without_dx_and_px as (
select
  mc.claim_id,
  mc.claim_line_number,
  mc.claim_type,
  mc.patient_id,
  mc.member_id,
  mc.claim_start_date,
  mc.claim_end_date,
  mc.claim_line_start_date,
  mc.claim_line_end_date,
  mc.admission_date,
  mc.discharge_date,
  mc.admit_source_code,
  mc.admit_type_code,
  mc.discharge_disposition_code,
  mc.place_of_service_code,
  mc.bill_type_code,
  mc.ms_drg_code,
  mc.apr_drg_code,
  mc.revenue_center_code,
  mc.service_unit_quantity,
  mc.hcpcs_code,
  mc.hcpcs_modifier_1,
  mc.hcpcs_modifier_2,
  mc.hcpcs_modifier_3,
  mc.hcpcs_modifier_4,
  mc.hcpcs_modifier_5,
  mc.rendering_npi,
  mc.billing_npi,
  mc.facility_npi,
  mc.paid_date,
  mc.paid_amount,
  mc.allowed_amount,
  mc.charge_amount,
  mc.total_cost_amount,
  eg.encounter_type,
  eg.encounter_id,
  eg.service_category_1,
  eg.service_category_2,
  mc.data_source
from {{ ref('input_layer__medical_claim') }} mc
left join {{ ref('claims_preprocessing__encounter_grouper') }} eg
    on eg.claim_id = mc.claim_id
    and eg.claim_line_number = mc.claim_line_number
    and eg.patient_id = mc.patient_id

)


select *
from select_relevant_columns_without_dx_and_px
