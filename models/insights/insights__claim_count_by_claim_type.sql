select 
    claim_type
    , count(distinct claim_id) 
from {{ ref('core__medical_claim') }}
group by claim_type
union all
select 
    'pharmacy'
    , count(distinct claim_id) 
from {{ ref('core__pharmacy_claim') }}