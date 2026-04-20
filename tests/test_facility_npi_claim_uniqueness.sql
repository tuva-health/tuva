with base as (
select distinct 
    claim_id
    , data_source
    , facility_npi 
from {{ ref('normalized_input__medical_claim') }}
)

select 
    claim_id
    , data_source
    , count(*) 
from base
group by 
    claim_id
    , data_source
having count(*) > 1
