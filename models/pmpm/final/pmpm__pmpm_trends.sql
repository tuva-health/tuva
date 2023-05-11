{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}

with medical_spend as (
select
  year_month,
  sum(paid_amount) as medical_spend
from {{ ref('pmpm__pmpm_output') }}
where
  had_eligibility_flag = 1
  and claim_type in ('institutional','professional')
group by year_month
order by year_month
),


inpatient_spend as (
select
  year_month,
  sum(paid_amount) as inpatient_spend
from {{ ref('pmpm__pmpm_output') }}
where
  had_eligibility_flag = 1
  and claim_type in ('institutional','professional')
  and service_category_1 = 'Inpatient'
group by year_month
order by year_month
),


outpatient_spend as (
select
  year_month,
  sum(paid_amount) as outpatient_spend
from {{ ref('pmpm__pmpm_output') }}
where
  had_eligibility_flag = 1
  and claim_type in ('institutional','professional')
  and service_category_1 = 'Outpatient'
group by year_month
order by year_month
),


office_visit_spend as (
select
  year_month,
  sum(paid_amount) as office_visit_spend
from {{ ref('pmpm__pmpm_output') }}
where
  had_eligibility_flag = 1
  and claim_type in ('institutional','professional')
  and service_category_1 = 'Office Visit'
group by year_month
order by year_month
),


ancillary_spend as (
select
  year_month,
  sum(paid_amount) as ancillary_spend
from {{ ref('pmpm__pmpm_output') }}
where
  had_eligibility_flag = 1
  and claim_type in ('institutional','professional')
  and service_category_1 = 'Ancillary'
group by year_month
order by year_month
),


other_spend as (
select
  year_month,
  sum(paid_amount) as other_spend
from {{ ref('pmpm__pmpm_output') }}
where
  had_eligibility_flag = 1
  and claim_type in ('institutional','professional')
  and service_category_1 = 'Other'
group by year_month
order by year_month
)


select
  aa.year_month,
  aa.medical_spend,
  zz.member_month_count,
  
  (aa.medical_spend * 1.0 / zz.member_month_count) as medical_pmpm,
  (bb.inpatient_spend * 1.0 / zz.member_month_count) as inpatient_pmpm,  
  (cc.outpatient_spend * 1.0 / zz.member_month_count) as outpatient_pmpm,
  (dd.office_visit_spend * 1.0 / zz.member_month_count) as office_visit_pmpm,
  (ee.ancillary_spend * 1.0 / zz.member_month_count) as ancillary_pmpm,  
  (ff.other_spend * 1.0 / zz.member_month_count) as other_pmpm
  
from medical_spend aa

     left join inpatient_spend bb
     on aa.year_month = bb.year_month

     left join outpatient_spend cc
     on aa.year_month = cc.year_month

     left join office_visit_spend dd
     on aa.year_month = dd.year_month

     left join ancillary_spend ee
     on aa.year_month = ee.year_month

     left join other_spend ff
     on aa.year_month = ff.year_month

     left join {{ ref('pmpm__member_month_count') }} zz
     on aa.year_month = zz.year_month
order by year_month
