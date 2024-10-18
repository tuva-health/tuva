{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
) }}


with cte as 
(
select sum(paid_amount) as paid_amount
,claim_type
from {{ ref('medical_claim') }}
group by claim_type

union 

select sum(paid_amount) as paid_amount
,'pharmacy' as claim_type
from {{ ref('pharmacy_claim') }}
)

,total_cte as 
(
select sum(paid_amount) as total_paid_amount
from cte
)

select claim_type
,paid_amount/total_paid_amount as percent_of_total_paid
from cte
cross join total_cte