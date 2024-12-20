select 
      person_id
    , payer 
    , year_month 
    , member_month
    , payment_risk_score
    , risk_adjusted_member_months
from {{ ref('medical_economics__risk_adjusted_member_months') }} 
