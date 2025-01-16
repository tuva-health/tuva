select 
      aa.person_id 
    , aa.claim_id
    , aa.encounter_id
    , aa.payer 
    , aa.claim_start_date
    , aa.claim_end_date 
    , aa.claim_line_number 
    , cc.service_category_id
    , aa.ms_drg_code
    , aa.apr_drg_code
    , aa.hcpcs_code
    , aa.rendering_id 
    , aa.paid_amount
    , aa.allowed_amount
    , bb.condition_grouper_id
    , dd.specialty_provider_id
from {{ ref('medical_economics__specialty_condition_grouper_medical_claim') }} aa
left join {{ ref('medical_economics__dim_condition_group') }} bb 
    on aa.condition_grouper_1 = bb.condition_grouper_1
    and aa.condition_grouper_2 = bb.condition_grouper_2
    and aa.condition_grouper_3 = bb.condition_grouper_3
left join {{ ref('medical_economics__dim_service_category') }} cc
    on aa.service_category_1 = cc.service_category_1
    and aa.service_category_2 = cc.service_category_2
    and aa.service_category_3 = cc.service_category_3
left join {{ ref('medical_economics__dim_specialty_provider') }} dd
    on aa.specialty_provider = dd.specialty_provider
