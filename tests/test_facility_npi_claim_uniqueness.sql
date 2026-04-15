with base as (
select distinct 
    claim_id
    , facility_npi 
from {{ ref('normalized_input__medical_claim') }}
)

select 
    claim_id
    , count(*) 
from base
group by 
    claim_id
having count(*) > 1
