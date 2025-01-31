with member_months as (

  select 
      aa.person_id
    , aa.payer
    , aa.year_month
    , bb.comparative_population_id
    , aa.member_month
    , aa.risk_adjusted_member_months
  from {{ ref('medical_economics__risk_adjusted_member_months') }} aa 
  left join {{ ref('medical_economics__fact_comparative_population') }} bb
    on aa.person_id = bb.person_id 

)

select 
      payer 
    , year_month 
    , comparative_population_id
    , sum(member_month) as member_months
    , sum(risk_adjusted_member_months) as risk_adjusted_member_months
from member_months
group by 1,2,3
