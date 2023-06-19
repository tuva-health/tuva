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


select
  claim_id,
  claim_line_number,
  claim_type,
  patient_id,
  member_id,
  claim_start_date,
  claim_end_date,
  claim_line_start_date,
  claim_line_end_date,
  admission_date,
  discharge_date,
  admit_source_code,
  admit_type_code,
  discharge_disposition_code,
  place_of_service_code,
  bill_type_code,
  ms_drg_code,
  apr_drg_code,
  revenue_center_code,
  service_unit_quantity,
  hcpcs_code,
  hcpcs_modifier_1,
  hcpcs_modifier_2,
  hcpcs_modifier_3,
  hcpcs_modifier_4,
  hcpcs_modifier_5,
  rendering_npi,
  billing_npi,
  facility_npi,
  paid_date,
  paid_amount,
  allowed_amount,
  charge_amount,
  total_cost_amount,
  data_source,
  '{{ var('last_update')}}' as last_update
from {{ ref('medical_claim') }} 
