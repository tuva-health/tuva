with member_months as (

  select 
      aa.person_id
    , aa.payer
    , aa.year_month
    , bb.comparative_population_id
    , aa.member_month
    , aa.risk_adjusted_member_months
    , cc.sex 
    , cc.age
    , coalesce(dd.high_cost_claimant_flag,0) as high_cost_claimant_flag
  from {{ ref('medical_economics__risk_adjusted_member_months') }} aa 
  left join {{ ref('medical_economics__fact_comparative_population') }} bb
    on aa.person_id = bb.person_id 
  left join {{ ref('medical_economics__dim_patient') }} cc
    on aa.person_id = cc.person_id 
  left join {{ ref('medical_economics__high_cost_claimant') }} dd
      on aa.person_id = dd.person_id  
      and aa.year_month = dd.year_month
      and aa.payer = dd.payer 

)

select 
      payer 
    , year_month 
    , comparative_population_id
    , sum(member_month) as member_months
    , sum(risk_adjusted_member_months) as risk_adjusted_member_months
    , sum(case when sex = 'female' then member_month end) / sum(member_month) as percent_female
    , avg(age) as average_age
    , sum(case when high_cost_claimant_flag = 0 then member_month end) as member_months_not_high_cost_claimant
from member_months
group by 1,2,3
