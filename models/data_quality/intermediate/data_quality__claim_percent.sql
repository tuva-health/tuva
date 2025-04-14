

with cte as 
(
select sum(paid_amount) as paid_amount
,claim_type
from {{ ref('input_layer__medical_claim') }}
group by claim_type

union all

select sum(paid_amount) as paid_amount
,'pharmacy' as claim_type
from {{ ref('input_layer__pharmacy_claim') }}
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