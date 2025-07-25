with normalized_input__medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'normalized_input__medical_claim') }}
)
select
    med.medical_claim_sk
    , med.claim_type
    , substring(med.bill_type_code, 1, 2) as bill_type_code -- not sure the substring() is needed
    , med.hcpcs_code
    , med.drg_code_type
    , med.drg_code
    , med.place_of_service_code
    , med.revenue_center_code
    , ccs.ccs_category
    , ccs.ccs_category_description
    , ccsr.default_ccsr_category_ip
    , ccsr.default_ccsr_category_op
    , ccsr.default_ccsr_category_description_ip
    , ccsr.default_ccsr_category_description_op
    , fac.primary_taxonomy_code
    , fac.primary_specialty_description as facility_primary_specialty_description
    , ren.primary_specialty_description as rend_primary_specialty_description
    , nitos.modality
    , coalesce(ms.medical_surgical, apr.medical_surgical) as medical_surgical
from normalized_input__medical_claim as med
    left outer join {{ ref('tuva_data_assets', 'hcpcs_ccs') }} as ccs
    on med.hcpcs_code = ccs.hcpcs_code
    left outer join {{ ref('tuva_data_assets', 'icd_10_cm_ccsr') }} as ccsr
    on med.diagnosis_code_1 = ccsr.icd_10_cm
    left outer join {{ ref('tuva_data_assets', 'npi') }} as fac
    on med.facility_npi = fac.npi
    left outer join {{ ref('tuva_data_assets', 'npi') }} as ren
    on med.rendering_npi = ren.npi
    left outer join {{ ref('tuva_data_assets', 'hcpcs_nitos') }} as nitos
    on med.hcpcs_code = nitos.hcpcs_code
    left outer join {{ ref('tuva_data_assets', 'ms_drg') }} as ms
    on med.drg_code = ms.ms_drg_code
    and med.drg_code_type = 'ms-drg'
    left outer join {{ ref('tuva_data_assets', 'apr_drg') }} as apr
    on med.drg_code = apr.apr_drg_code
    and med.drg_code_type = 'apr-drg'
