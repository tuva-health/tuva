{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
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

with all_encounters as (
    select   claim_id
, claim_line_number
, encounter_id
, encounter_type
, encounter_group
    from {{ ref('encounters__combined_claim_line_crosswalk') }}
    where claim_line_attribution_number = 1
    
    union all 

    select  
  claim_id
, claim_line_number
, encounter_id
, encounter_type
, encounter_group
    from {{ ref('encounters__orphaned_claims') }}
)

select
    {{  concat_custom([
        "med.claim_id",
        "'-'",
        "med.claim_line_number",
        "'-'",
        "med.data_source"
    ]) }} as medical_claim_id
    , cast(med.claim_id as {{ dbt.type_string() }} ) as claim_id
    , cast(med.claim_line_number as {{ dbt.type_int() }} ) as claim_line_number
    , cast(x.encounter_id as {{ dbt.type_string() }} ) as encounter_id
    , cast(x.encounter_type as {{ dbt.type_string() }} ) as encounter_type
    , cast(x.encounter_group as {{ dbt.type_string() }} ) as encounter_group
    , cast(med.claim_type as {{ dbt.type_string() }} ) as claim_type
    , cast(med.person_id as {{ dbt.type_string() }} ) as person_id
    , cast(med.member_id as {{ dbt.type_string() }} ) as member_id
    , cast(med.payer as {{ dbt.type_string() }} ) as payer
    , med.{{ quote_column('plan') }}
    , {{ try_to_cast_date('med.claim_start_date', 'YYYY-MM-DD') }} as claim_start_date
    , {{ try_to_cast_date('med.claim_end_date', 'YYYY-MM-DD') }} as claim_end_date
    , {{ try_to_cast_date('med.claim_line_start_date', 'YYYY-MM-DD') }} as claim_line_start_date
    , {{ try_to_cast_date('med.claim_line_end_date', 'YYYY-MM-DD') }} as claim_line_end_date
    , {{ try_to_cast_date('med.admission_date', 'YYYY-MM-DD') }} as admission_date
    , {{ try_to_cast_date('med.discharge_date', 'YYYY-MM-DD') }} as discharge_date
    , cast(srv_group.service_category_1 as {{ dbt.type_string() }} ) as service_category_1
    , cast(srv_group.service_category_2 as {{ dbt.type_string() }} ) as service_category_2
    , cast(srv_group.service_category_3 as {{ dbt.type_string() }} ) as service_category_3
    , cast(med.admit_source_code as {{ dbt.type_string() }} ) as admit_source_code
    , cast(med.admit_source_description as {{ dbt.type_string() }} ) as admit_source_description
    , cast(med.admit_type_code as {{ dbt.type_string() }} ) as admit_type_code
    , cast(med.admit_type_description as {{ dbt.type_string() }} ) as admit_type_description
    , cast(med.discharge_disposition_code as {{ dbt.type_string() }} ) as discharge_disposition_code
    , cast(med.discharge_disposition_description as {{ dbt.type_string() }} ) as discharge_disposition_description
    , cast(med.place_of_service_code as {{ dbt.type_string() }} ) as place_of_service_code
    , cast(med.place_of_service_description as {{ dbt.type_string() }} ) as place_of_service_description
    , cast(med.bill_type_code as {{ dbt.type_string() }} ) as bill_type_code
    , cast(med.bill_type_description as {{ dbt.type_string() }} ) as bill_type_description
    , cast(med.drg_code_type as {{ dbt.type_string() }} ) as drg_code_type
    , cast(med.drg_code as {{ dbt.type_string() }} ) as drg_code
    , cast(med.drg_description as {{ dbt.type_string() }} ) as drg_description
    , cast(med.revenue_center_code as {{ dbt.type_string() }} ) as revenue_center_code
    , cast(med.revenue_center_description as {{ dbt.type_string() }} ) as revenue_center_description
    , cast(med.service_unit_quantity as {{ dbt.type_numeric() }} ) as service_unit_quantity
    , cast(med.hcpcs_code as {{ dbt.type_string() }} ) as hcpcs_code
    , cast(med.hcpcs_modifier_1 as {{ dbt.type_string() }} ) as hcpcs_modifier_1
    , cast(med.hcpcs_modifier_2 as {{ dbt.type_string() }} ) as hcpcs_modifier_2
    , cast(med.hcpcs_modifier_3 as {{ dbt.type_string() }} ) as hcpcs_modifier_3
    , cast(med.hcpcs_modifier_4 as {{ dbt.type_string() }} ) as hcpcs_modifier_4
    , cast(med.hcpcs_modifier_5 as {{ dbt.type_string() }} ) as hcpcs_modifier_5
    , cast(med.rendering_id as {{ dbt.type_string() }} ) as rendering_id
    , cast(med.rendering_tin as {{ dbt.type_string() }} ) as rendering_tin
    , cast(med.rendering_name as {{ dbt.type_string() }} ) as rendering_name
    , cast(med.billing_id as {{ dbt.type_string() }} ) as billing_id
    , cast(med.billing_tin as {{ dbt.type_string() }} ) as billing_tin
    , cast(med.billing_name as {{ dbt.type_string() }} ) as billing_name
    , cast(med.facility_id as {{ dbt.type_string() }} ) as facility_id
    , cast(med.facility_name as {{ dbt.type_string() }} ) as facility_name
    , {{ try_to_cast_date('med.paid_date', 'YYYY-MM-DD') }} as paid_date
    , cast(med.paid_amount as {{ dbt.type_numeric() }} ) as paid_amount
    , cast(med.allowed_amount as {{ dbt.type_numeric() }} ) as allowed_amount
    , cast(med.charge_amount as {{ dbt.type_numeric() }} ) as charge_amount
    , cast(med.coinsurance_amount as {{ dbt.type_numeric() }} ) as coinsurance_amount
    , cast(med.copayment_amount as {{ dbt.type_numeric() }} ) as copayment_amount
    , cast(med.deductible_amount as {{ dbt.type_numeric() }} ) as deductible_amount
    , cast(med.total_cost_amount as {{ dbt.type_numeric() }} ) as total_cost_amount
    , cast(med.in_network_flag as int ) as in_network_flag
    , cast(
    case
        when enroll.claim_id is not null then 1
            else 0
    end as int) as enrollment_flag
    , enroll.member_month_key
    , cast(med.data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
from {{ ref('normalized_input__medical_claim') }} as med
inner join {{ ref('service_category__service_category_grouper') }} as srv_group
  on med.claim_id = srv_group.claim_id
  and med.claim_line_number = srv_group.claim_line_number
  and srv_group.duplicate_row_number = 1
inner join all_encounters as x
  on med.claim_id = x.claim_id
  and med.claim_line_number = x.claim_line_number
left join {{ ref('claims_enrollment__flag_claims_with_enrollment') }} as enroll
  on med.claim_id = enroll.claim_id
  and med.claim_line_number = enroll.claim_line_number
  and med.person_id = enroll.person_id
  and med.payer = enroll.payer
  and med.{{ quote_column('plan') }} = enroll.{{ quote_column('plan') }}
  and med.data_source = enroll.data_source