with member_months as (

    select 
          aa.person_id
        , aa.payer 
        , aa.year_month 
        , aa.member_month
        , bb.payment_risk_score
        , aa.member_month * bb.payment_risk_score as risk_adjusted_member_months
    from {{ ref('medical_economics__stg_core_member_months') }} aa
    left join {{ ref('medical_economics__stg_cms_hcc_patient_risk_scores') }} bb
        on aa.person_id = bb.person_id 
        and left(aa.year_month,4) = bb.payment_year

)

select *
from member_months 