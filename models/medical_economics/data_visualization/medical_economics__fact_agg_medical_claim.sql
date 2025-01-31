
with fact_medical_claim as (

  select 
      aa.claim_id  
    , aa.encounter_id
    , aa.payer
    , aa.claim_start_date
    , replace(left(aa.claim_start_date,7),'-','') as year_month
    , aa.service_category_id
    , aa.condition_grouper_id
    , aa.specialty_provider_id
    , bb.comparative_population_id
    , aa.paid_amount
    , aa.allowed_amount
  from {{ ref('medical_economics__fact_medical_claim') }} aa
  left join {{ ref('medical_economics__fact_comparative_population') }} bb
      on aa.person_id = bb.person_id  

)

select 
      payer 
    , year_month
    , comparative_population_id
    , service_category_id
    , condition_grouper_id
    , specialty_provider_id
    , sum(paid_amount) as paid_amount
    , sum(allowed_amount) as allowed_amount
    , count(distinct claim_id) as distinct_claim_id
    , count(distinct encounter_id) as distinct_encounter_id
from fact_medical_claim
group by 1,2,3,4,5,6