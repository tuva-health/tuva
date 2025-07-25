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
    , med.member_id
    , {{ dbt_utils.generate_surrogate_key(['med.data_source', 'med.member_id']) }} as patient_sk
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
    , coalesce(ms_drg.ms_drg_description, apr_drg.apr_drg_description) as drg_description
    , coalesce(ms_drg.medical_surgical, apr_drg.medical_surgical) as medical_surgical
    , med.admit_source_code
    , ad_src.admit_source_description
    , med.admit_type_code
    , ad_typ.admit_type_description
    , med.place_of_service_code
    , pos.place_of_service_description
    , med.revenue_center_code
    , rev.revenue_center_description
    , med.diagnosis_code_type as primary_diagnosis_code_type
    , med.diagnosis_code_1 as primary_diagnosis_code
    , coalesce(icd10cm.long_description, icd9cm.long_description) as primary_diagnosis_description
    , med.billing_npi
    , med.rendering_npi
    , med.facility_npi
    , fac.provider_organization_name as facility_name
    , fac.primary_specialty_description as facility_type
    , med.discharge_disposition_code
    , dd.discharge_disposition_description
    , med.paid_amount
    , med.charge_amount
    , med.allowed_amount
from {{ ref('normalized_input__medical_claim') }} as med
    left join {{ ref('the_tuva_project', 'service_category__medical_claim_service_category') }} as service_category
    on med.medical_claim_sk = service_category.medical_claim_sk
    and service_category.priority = 1
    left outer join {{ ref('tuva_data_assets', 'bill_type') }} as bt
    on med.bill_type_code = bt.bill_type_code
    left outer join {{ ref('tuva_data_assets', 'hcpcs_ccs') }} as ccs
    on med.hcpcs_code = ccs.hcpcs_code
    left outer join {{ ref('tuva_data_assets', 'place_of_service') }} as pos
    on med.place_of_service_code = pos.place_of_service_code
    left outer join {{ ref('tuva_data_assets', 'revenue_center') }} as rev
    on med.revenue_center_code = rev.revenue_center_code
    left outer join {{ ref('tuva_data_assets', 'npi') }} as fac
    on med.facility_npi = fac.npi
    left outer join {{ ref('tuva_data_assets', 'discharge_disposition') }} as dd
    on med.discharge_disposition_code = dd.discharge_disposition_code
    left outer join {{ ref('tuva_data_assets', 'admit_source') }} as ad_src
    on med.admit_source_code = ad_src.admit_source_code
    left outer join {{ ref('tuva_data_assets', 'admit_type') }} as ad_typ
    on med.admit_type_code = ad_typ.admit_type_code
    left outer join {{ ref('tuva_data_assets', 'ms_drg') }} as ms_drg
    on med.drg_code_type = 'ms-drg'
    and med.drg_code = ms_drg.ms_drg_code
    left outer join {{ ref('tuva_data_assets', 'apr_drg') }} as apr_drg
    on med.drg_code_type = 'apr-drg'
    and med.drg_code = apr_drg.apr_drg_code
    left outer join {{ ref('tuva_data_assets', 'icd_10_cm') }} as icd10cm
    on med.diagnosis_code_1 = icd10cm.icd_10_cm
    and med.diagnosis_code_type = 'icd-10-cm'
    left outer join {{ ref('tuva_data_assets', 'icd_9_cm') }} as icd9cm
    on med.diagnosis_code_1 = icd9cm.icd_9_cm
    and med.diagnosis_code_type = 'icd-9-cm'
