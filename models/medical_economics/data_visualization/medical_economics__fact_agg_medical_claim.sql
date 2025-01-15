select 
      payer 
    , replace(left(claim_start_date,7),'-','') as year_month
    , service_category_id
    , condition_grouper_id
    , specialty_provider_id
    , sum(paid_amount) as paid_amount
    , sum(allowed_amount) as allowed_amount
    , count(distinct claim_id) as distinct_claim_id
from {{ ref('medical_economics__fact_medical_claim') }} 
group by 1,2,3,4,5