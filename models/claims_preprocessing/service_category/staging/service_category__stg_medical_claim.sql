with normalized_input__medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'normalized_input__medical_claim') }}
)
select
    m.medical_claim_sk
--    , m.data_source
--    , m.claim_id
--    , m.claim_line_number
    , m.claim_type
--    , coalesce(m.admission_date, m.claim_line_start_date, m.claim_start_date) as start_date
--    , coalesce(m.discharge_date, m.claim_line_end_date, m.claim_end_date) as end_date
--    , m.admission_date
--    , m.discharge_date
--    , m.claim_start_date
--    , m.claim_end_date
--    , m.claim_line_start_date
--    , m.claim_line_end_date
    , substring(m.bill_type_code, 1, 2) as bill_type_code -- not sure the substring() is needed
--    , bt.bill_type_description
    , m.hcpcs_code
    , ccs.ccs_category
    , ccs.ccs_category_description
    , m.drg_code_type
    , m.drg_code
--    , m.drg_description
    , m.place_of_service_code
--    , pos.place_of_service_description
    , substring(m.revenue_center_code, 1, 3) as revenue_center_code -- not sure the substring() is needed
--    , rev.revenue_center_description
--    , m.diagnosis_code_1
    , dx.default_ccsr_category_ip
    , dx.default_ccsr_category_op
    , dx.default_ccsr_category_description_ip
    , dx.default_ccsr_category_description_op
    , fac.primary_taxonomy_code
    , fac.primary_specialty_description as facility_primary_specialty_description
    , ren.primary_specialty_description as rend_primary_specialty_description
    , nitos.modality
from normalized_input__medical_claim as m
    left outer join {{ ref('tuva_data_assets', 'bill_type') }} as bt
    on m.bill_type_code = bt.bill_type_code
    left outer join {{ ref('tuva_data_assets', 'hcpcs_ccs') }} as ccs
    on m.hcpcs_code = ccs.hcpcs_code
    left outer join {{ ref('tuva_data_assets', 'place_of_service') }} as pos
    on m.place_of_service_code = pos.place_of_service_code
    left outer join {{ ref('tuva_data_assets', 'revenue_center') }} as rev
    on m.revenue_center_code = rev.revenue_center_code
    left outer join {{ ref('tuva_data_assets', 'icd_10_cm_ccsr') }} as dx
    on m.diagnosis_code_1 = dx.icd_10_cm_code
    left outer join {{ ref('tuva_data_assets', 'npi') }} as fac
    on m.facility_npi = fac.npi
    left outer join {{ ref('tuva_data_assets', 'npi') }} as ren
    on m.rendering_npi = ren.npi
    left outer join {{ ref('tuva_data_assets', 'hcpcs_nitos') }} as nitos
    on m.hcpcs_code = nitos.hcpcs_code
