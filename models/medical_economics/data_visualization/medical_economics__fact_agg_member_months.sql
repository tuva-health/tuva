select 
      payer 
    , year_month 
    , sum(member_month) as member_months
    , sum(risk_adjusted_member_months) as risk_adjusted_member_months
from {{ ref('medical_economics__risk_adjusted_member_months') }} 
group by 1,2
