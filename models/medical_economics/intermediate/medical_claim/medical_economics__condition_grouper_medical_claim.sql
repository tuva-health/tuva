with medical_claim as (

    select distinct
          aa.person_id
        , aa.claim_id
        , bb.condition_grouper_1 
        , bb.condition_grouper_2
        , bb.condition_grouper_3
    from {{ ref('medical_economics__stg_core_medical_claim') }} aa
    left join {{ ref('medical_economics__stg_ccsr_long_condition_category') }} bb
        on aa.person_id = bb.person_id
        and aa.claim_id = bb.claim_id 

)

select *
from medical_claim 