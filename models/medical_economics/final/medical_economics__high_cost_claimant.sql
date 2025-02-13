with fact_medical_claim as (

  select 
      aa.claim_id  
    , aa.person_id 
    , aa.payer 
    , replace(left(aa.claim_start_date,7),'-','') as year_month
    , aa.paid_amount
    , aa.allowed_amount
  from {{ ref('medical_economics__specialty_condition_grouper_medical_claim') }} aa

),

-- member_months_base as (

--   select 
--       aa.person_id
--     , aa.payer
--     , aa.year_month
--     , aa.member_month
--     , aa.risk_adjusted_member_months
--   from {{ ref('medical_economics__risk_adjusted_member_months') }} aa 

-- ),

-- member_months as (

--     select 
--           payer 
--         , year_month 
--         , sum(member_month) as member_months
--     from member_months_base
--     group by 1,2

-- ),

paid_amount_summarize as (

  select 
      person_id 
    , payer 
    , year_month 
    , sum(paid_amount) as total_paid
  from fact_medical_claim
  group by 1,2,3

),

high_cost_claimant_flag as (

  select 
      aa.person_id 
    , aa.payer 
    , aa.year_month 
    , aa.total_paid 
    , sum(total_paid) over (
        partition by person_id, payer 
        order by year_month 
        rows between 11 preceding and current row
    ) as rolling_12_paid_amount 
    , case 
        when rolling_12_paid_amount >= 150000
        then 1
        else 0
    end as high_cost_claimant_flag
  from paid_amount_summarize aa 

)

select * 
from high_cost_claimant_flag