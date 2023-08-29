{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
   )
}}

-- *************************************************
-- This dbt model creates the medical_claim table
-- in core. It adds these 4 fields to the input layer
-- medical claim table:
--      encounter_id
--      service_category_1
--      service_category_2
-- *************************************************


select
    med.claim_id
    , med.claim_line_number
    , enc.encounter_id
    , med.claim_type
    , med.patient_id
    , med.member_id
    , med.claim_start_date
    , med.claim_end_date
    , med.claim_line_start_date
    , med.claim_line_end_date
    , med.admission_date
    , med.discharge_date
    , srv_group.service_category_1
    , srv_group.service_category_2
    , med.admit_source_code
    , med.admit_type_code
    , med.discharge_disposition_code
    , med.place_of_service_code
    , med.bill_type_code
    , med.ms_drg_code
    , med.apr_drg_code
    , med.revenue_center_code
    , med.service_unit_quantity
    , med.hcpcs_code
    , med.hcpcs_modifier_1
    , med.hcpcs_modifier_2
    , med.hcpcs_modifier_3
    , med.hcpcs_modifier_4
    , med.hcpcs_modifier_5
    , med.rendering_npi
    , med.billing_npi
    , med.facility_npi
    , med.paid_date
    , med.paid_amount
    , med.allowed_amount
    , med.charge_amount
    , med.total_cost_amount
    , med.data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim') }} med
inner join {{ ref('service_category__service_category_grouper') }} srv_group
    on med.claim_id = srv_group.claim_id
    and med.claim_line_number = srv_group.claim_line_number
left join {{ ref('acute_inpatient__encounter_id') }} enc
    on med.claim_id = enc.claim_id
