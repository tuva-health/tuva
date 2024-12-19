select 
      aa.person_id
    , aa.claim_id 
    , aa.payer 
    , aa.claim_line_number
    , aa.prescribing_provider_id
    , aa.dispensing_date
    , aa.ndc_code
    , aa.ndc_description
    , aa.quantity
    , aa.days_supply
    , aa.refills
    , aa.paid_amount
    , aa.allowed_amount
    , bb.condition_grouper_1 
    , bb.condition_grouper_2
    , bb.condition_grouper_3
    , cc.specialty_provider
from {{ ref('medical_economics__stg_core_pharmacy_claim') }} aa
left join {{ ref('medical_economics__condition_grouper_pharmacy_claim') }} bb
    on aa.person_id = bb.person_id
    and aa.claim_id = bb.claim_id 
left join {{ ref('medical_economics__specialty_pharmacy_claim') }} cc
    on aa.person_id = cc.person_id
    and aa.claim_id = cc.claim_id 