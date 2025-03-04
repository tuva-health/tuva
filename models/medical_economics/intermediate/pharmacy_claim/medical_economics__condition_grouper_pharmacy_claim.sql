--For future development of condition grouper for pharmacy claims

with pharmacy_claim as (

    select distinct 
          person_id
        , claim_id 
        , null as condition_grouper_1 
        , null as condition_grouper_2
        , null as condition_grouper_3
    from {{ ref('medical_economics__pharmacy_claim_intermediate') }} 

)

select *
from pharmacy_claim 