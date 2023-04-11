{{ config(enabled = var('pmpm_enabled',var('tuva_packages_enabled',True)) ) }}

with member_months as
(
    select distinct patient_id, year_month
    from {{ref('pmpm__member_months')}}
)
, claim_spend_and_utilization as
(
    select *
    from {{ref('pmpm__claim_spend_and_utilization')}}
)
, cte_spend_and_visits as
(
    select 
        patient_id
        ,year_month
        ,sum(paid) as total_paid
        ,sum(case when claim_type <> 'pharmacy' then paid else 0 end) as medical_paid
        ,sum(case when claim_type = 'pharmacy' then paid else 0 end) as pharmacy_paid

    from claim_spend_and_utilization
    group by
        patient_id
        ,year_month 
)

select 
    mm.patient_id
    ,mm.year_month
    --,plan or payer field
    ,coalesce(sv.total_paid,0) as total_paid
    ,coalesce(sv.medical_paid,0) as medical_paid
    ,coalesce(sv.pharmacy_paid,0) as pharmacy_paid
from member_months mm
left join cte_spend_and_visits sv
    on mm.patient_id = sv.patient_id
    and mm.year_month = sv.year_month