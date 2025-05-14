with encounter_sk as (
    select
        e.encounter_id
        ,e.encounter_group_sk
        ,e.encounter_type_sk
        ,e.primary_diagnosis_code
        ,e.primary_diagnosis_description
        ,primary_provider_id
        ,provider_type
        ,specialty
        ,ccsr.ccsr_parent_category
        ,ccsr.ccsr_category
        ,ccsr.ccsr_category_description
    from {{ ref('power_bi__fact_encounters') }} e
    inner join {{ ref('power_bi__dim_encounter_provider') }} p on e.encounter_id = p.encounter_id
    left join {{ ref('ccsr__dx_vertical_pivot') }} ccsr on e.primary_diagnosis_code = ccsr.code
        and ccsr.ccsr_category_rank = 1
)

select
    medical_claim_id
    , mc.encounter_id
    , encounter_group_sk
    , encounter_type_sk
    , primary_diagnosis_code
    , primary_diagnosis_description
    ,primary_provider_id
    ,provider_type
    ,specialty
    , ccsr_parent_category
    , ccsr_category
    , ccsr_category_description
    , person_id
    , {{ dbt.concat(["person_id", "'|'", "data_source"]) }} as patient_source_key
    , TO_CHAR(claim_start_date, 'YYYYMM') as year_month
    , sc.service_category_sk
    , claim_id
    , claim_line_number
    , claim_type
    , payer
    , {{ quote_column('plan') }}
    , claim_start_date
    , claim_end_date
    , claim_line_start_date
    , claim_line_end_date
    , admission_date
    , discharge_date
    , admit_source_code
    , admit_source_description
    , admit_type_code
    , admit_type_description
    , discharge_disposition_code
    , discharge_disposition_description
    , place_of_service_code
    , place_of_service_description
    , bill_type_code
    , bill_type_description
    , drg_code_type
    , drg_code
    , drg_description
    , revenue_center_code
    , revenue_center_description
    , service_unit_quantity
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
    , rendering_id
    , rendering_tin
    , rendering_name
    , billing_id
    , billing_tin
    , billing_name
    , facility_id
    , facility_name
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , total_cost_amount
    , in_network_flag
    , data_source
    , tuva_last_run
from {{ ref('core__medical_claim') }} mc
INNER JOIN {{ ref('power_bi__dim_service_category') }} sc on mc.service_category_1 = sc.service_category_1
    AND mc.service_category_2 = sc.service_category_2
    AND mc.service_category_3 = sc.service_category_3
INNER JOIN encounter_sk esk on mc.encounter_id = esk.encounter_id
WHERE enrollment_flag = 1