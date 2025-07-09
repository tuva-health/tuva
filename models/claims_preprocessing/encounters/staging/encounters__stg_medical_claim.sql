{{
    config(
        materialized='table'
    )
}}
with normalized_input__medical_claim as (
    select *
    from {{ ref('normalized_input__medical_claim') }}
)
select
    med.medical_claim_sk
    , med.data_source
    , pat_id.member_data_source_sk as patient_data_source_id --TODO: Does this work better?
    , med.member_id
    , med.claim_id
    , med.claim_line_number
    , med.claim_type
    , coalesce(med.admission_date, med.claim_line_start_date, med.claim_start_date) as start_date
    , coalesce(med.discharge_date, med.claim_line_end_date, med.claim_end_date) as end_date
    , med.admission_date
    , med.discharge_date
    , med.claim_start_date
    , med.claim_end_date
    , med.claim_line_start_date
    , med.claim_line_end_date
    , service_category.service_category_1
    , service_category.service_category_2
    , service_category.service_category_3
    , substring(med.bill_type_code, 1, 2) as bill_type_code -- not sure the substring() is needed
    , bt.bill_type_description
    , med.hcpcs_code
    , med.hcpcs_modifier_1
    , med.hcpcs_modifier_2
    , med.hcpcs_modifier_3
    , med.hcpcs_modifier_4
    , med.hcpcs_modifier_5
    , ccs.ccs_category
    , ccs.ccs_category_description
    , med.drg_code_type
    , med.drg_code
    --  , coalesce(msdrg.ms_drg_description, aprdrg.apr_drg_description) as drg_description
    , med.admit_source_code
    , med.admit_type_code
    , med.place_of_service_code
    --  , pos.place_of_service_description
    , substring(med.revenue_center_code, 1, 3) as revenue_center_code -- not sure the substring() is needed
    --  , r.revenue_center_description
    , med.diagnosis_code_type
    , med.diagnosis_code_1
    --  , dx.default_ccsr_category_ip
    --  , dx.default_ccsr_category_op
    --  , dx.default_ccsr_category_description_ip
    --  , dx.default_ccsr_category_description_op
    --  , p.primary_taxonomy_code
    --  , p.primary_specialty_description
    --  , n.modality
    , med.billing_npi
    , med.rendering_npi
    --  , rend.primary_specialty_description as rend_primary_specialty_description
    , med.facility_npi
    , med.discharge_disposition_code
    , med.paid_amount
    , med.charge_amount
    , med.allowed_amount
from {{ ref('normalized_input__medical_claim') }} as med
    left join {{ ref('the_tuva_project', 'service_category__medical_claim_service_category') }} as service_category
    on med.medical_claim_sk = service_category.medical_claim_sk
    and service_category.priority = 1
    left join {{ ref('the_tuva_project', 'enrollment__medical_claim_member_month') }} as member_month
    on med.medical_claim_sk = member_month.medical_claim_sk

    left join {{ ref('the_tuva_project', 'encounters__patient_data_source_id') }} as pat_id
    on med.member_id = pat_id.member_id
    and med.data_source = pat_id.data_source
    
    left outer join {{ ref('tuva_data_assets', 'bill_type') }} as bt
    on med.bill_type_code = bt.bill_type_code
    left outer join {{ ref('tuva_data_assets', 'hcpcs_ccs') }} as ccs
    on med.hcpcs_code = ccs.hcpcs_code
    left outer join {{ ref('tuva_data_assets', 'place_of_service') }} as pos
    on med.place_of_service_code = pos.place_of_service_code
    left outer join {{ ref('tuva_data_assets', 'revenue_center') }} as rev
    on med.revenue_center_code = rev.revenue_center_code
    left outer join {{ ref('tuva_data_assets', 'icd_10_cm_ccsr') }} as dx
    on med.diagnosis_code_1 = dx.icd_10_cm_code
    left outer join {{ ref('tuva_data_assets', 'npi') }} as fac
    on med.facility_npi = fac.npi
    left outer join {{ ref('tuva_data_assets', 'npi') }} as ren
    on med.rendering_npi = ren.npi
    left outer join {{ ref('tuva_data_assets', 'hcpcs_nitos') }} as nitos
    on med.hcpcs_code = nitos.hcpcs_code