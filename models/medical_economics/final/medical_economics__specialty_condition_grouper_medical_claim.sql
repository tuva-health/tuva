select 
      aa.person_id
    , aa.claim_id
    , aa.encounter_id
    , aa.payer 
    , aa.claim_start_date
    , aa.claim_end_date
    , aa.claim_line_number
    , aa.service_category_1
    , aa.service_category_2
    , aa.service_category_3
    , aa.ms_drg_code
    , aa.apr_drg_code
    , aa.hcpcs_code
    , aa.rendering_id
    , aa.paid_amount
    , aa.allowed_amount
    , bb.condition_grouper_1 
    , bb.condition_grouper_2
    , bb.condition_grouper_3
    , cc.specialty_provider
from {{ ref('medical_economics__stg_core_medical_claim') }} aa
left join {{ ref('medical_economics__condition_grouper_medical_claim') }} bb
    on aa.person_id = bb.person_id
    and aa.claim_id = bb.claim_id 
left join {{ ref('medical_economics__specialty_medical_claim') }} cc
    on aa.person_id = cc.person_id
    and aa.claim_id = cc.claim_id 