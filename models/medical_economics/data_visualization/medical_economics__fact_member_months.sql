with member_months as (

  select 
      aa.person_id
    , aa.payer
    , to_date(aa.year_month || '01','YYYYMMDD') as year_month
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

select *
from member_months