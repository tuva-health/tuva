-- depends_on: {{ ref('data_quality__claims_preprocessing_summary') }}

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
    cast(med.claim_id as {{ dbt.type_string() }} ) as claim_id
    , cast(med.claim_line_number as {{ dbt.type_int() }} ) as claim_line_number
    , cast(coalesce(ap.encounter_id,ed.encounter_id) as {{ dbt.type_string() }} ) as encounter_id 
    , cast(med.claim_type as {{ dbt.type_string() }} ) as claim_type
    , cast(med.patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(med.member_id as {{ dbt.type_string() }} ) as member_id
    , cast(med.payer as {{ dbt.type_string() }} ) as payer
    , cast(med.plan as {{ dbt.type_string() }} ) as plan
    , {{ try_to_cast_date('med.claim_start_date', 'YYYY-MM-DD') }} as claim_start_date
    , {{ try_to_cast_date('med.claim_end_date', 'YYYY-MM-DD') }} as claim_end_date
    , {{ try_to_cast_date('med.claim_line_start_date', 'YYYY-MM-DD') }} as claim_line_start_date
    , {{ try_to_cast_date('med.claim_line_end_date', 'YYYY-MM-DD') }} as claim_line_end_date
    , {{ try_to_cast_date('med.admission_date', 'YYYY-MM-DD') }} as admission_date
    , {{ try_to_cast_date('med.discharge_date', 'YYYY-MM-DD') }} as discharge_date
    , cast(srv_group.service_category_1 as {{ dbt.type_string() }} ) as service_category_1
    , cast(srv_group.service_category_2 as {{ dbt.type_string() }} ) as service_category_2
    , cast(med.admit_source_code as {{ dbt.type_string() }} ) as admit_source_code
    , cast(med.admit_type_code as {{ dbt.type_string() }} ) as admit_type_code
    , cast(med.discharge_disposition_code as {{ dbt.type_string() }} ) as discharge_disposition_code
    , cast(med.place_of_service_code as {{ dbt.type_string() }} ) as place_of_service_code
    , cast(med.bill_type_code as {{ dbt.type_string() }} ) as bill_type_code
    , cast(med.ms_drg_code as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(med.apr_drg_code as {{ dbt.type_string() }} ) as apr_drg_code
    , cast(med.revenue_center_code as {{ dbt.type_string() }} ) as revenue_center_code
    , cast(med.service_unit_quantity as {{ dbt.type_int() }} ) as service_unit_quantity
    , cast(med.hcpcs_code as {{ dbt.type_string() }} ) as hcpcs_code
    , cast(med.hcpcs_modifier_1 as {{ dbt.type_string() }} ) as hcpcs_modifier_1
    , cast(med.hcpcs_modifier_2 as {{ dbt.type_string() }} ) as hcpcs_modifier_2
    , cast(med.hcpcs_modifier_3 as {{ dbt.type_string() }} ) as hcpcs_modifier_3
    , cast(med.hcpcs_modifier_4 as {{ dbt.type_string() }} ) as hcpcs_modifier_4
    , cast(med.hcpcs_modifier_5 as {{ dbt.type_string() }} ) as hcpcs_modifier_5
    , cast(med.rendering_npi as {{ dbt.type_string() }} ) as rendering_npi
    , cast(med.billing_npi as {{ dbt.type_string() }} ) as billing_npi
    , cast(med.facility_npi as {{ dbt.type_string() }} ) as facility_npi
    , {{ try_to_cast_date('med.paid_date', 'YYYY-MM-DD') }} as paid_date
    , cast(med.paid_amount as {{ dbt.type_numeric() }} ) as paid_amount
    , cast(med.allowed_amount as {{ dbt.type_numeric() }} ) as allowed_amount
    , cast(med.charge_amount as {{ dbt.type_numeric() }} ) as charge_amount
    , cast(med.coinsurance_amount as {{ dbt.type_numeric() }} ) as coinsurance_amount
    , cast(med.copayment_amount as {{ dbt.type_numeric() }} ) as copayment_amount
    , cast(med.deductible_amount as {{ dbt.type_numeric() }} ) as deductible_amount
    , cast(med.total_cost_amount as {{ dbt.type_numeric() }} ) as total_cost_amount
    , cast(med.data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
from {{ ref('normalized_input__medical_claim') }} med
left join {{ ref('service_category__service_category_grouper') }} srv_group
    on med.claim_id = srv_group.claim_id
    and med.claim_line_number = srv_group.claim_line_number
left join {{ ref('acute_inpatient__encounter_id') }} ap
    on med.claim_id = ap.claim_id
    and med.claim_line_number = ap.claim_line_number
left join {{ ref('emergency_department__int_encounter_id') }} ed
    on med.claim_id = ed.claim_id
    and med.claim_line_number = ed.claim_line_number

