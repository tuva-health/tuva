with medical_claim as (

    select distinct 
          aa.person_id
        , aa.claim_id
        , bb.primary_specialty_description as specialty_provider
    from {{ ref('medical_economics__stg_core_medical_claim') }} aa
    left join {{ ref('medical_economics__stg_terminology_provider') }} bb
        on aa.rendering_id = bb.npi 

)

select *
from medical_claim 