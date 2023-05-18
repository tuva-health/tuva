{{ config(
     enabled = var('claims_date_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

with med_claims as (
select
  min(claim_start_date) as claim_start_date
, min(claim_end_date) as claim_end_date
, min(admission_date) as admission_date
, min(discharge_date) as discharge_date
, min(paid_date) as paid_date
, claim_id
from {{ ref('medical_claim') }} 
group by claim_id
)

, transform as (
select
  date_part(year,claim_start_date) || lpad(date_part(month,claim_start_date),2,0) as claim_start_date
, date_part(year,claim_end_date) || lpad(date_part(month,claim_end_date),2,0) as claim_end_date
, date_part(year,admission_date) || lpad(date_part(month,admission_date),2,0) as admission_date
, date_part(year,discharge_date) || lpad(date_part(month,discharge_date),2,0) as discharge_date
, date_part(year,paid_date) || lpad(date_part(month,paid_date),2,0) as paid_date
, claim_id
from med_claims
)

, pivot_prep as (
select
  claim_start_date as year_month
, 'claim_start_date' as date_type
, count(distinct claim_id) as cnt
from transform 
group by 1,2

union all

select
  claim_end_date as year_month
, 'claim_end_date' as date_type
, count(distinct claim_id) as cnt
from transform 
group by 1,2

union all

select
  admission_date as year_month
, 'admission_date' as date_type
, count(distinct claim_id) as cnt
from transform 
group by 1,2

union all

select
  discharge_date as year_month
, 'discharge_date' as date_type
, count(distinct claim_id) as cnt
from transform 
group by 1,2

union all

select
  paid_date as year_month
, 'paid_date' as date_type
, count(distinct claim_id) as cnt
from transform 
group by 1,2
)

select *
from pivot_prep
pivot (sum(cnt) for date_type in (
    'claim_start_date',
    'claim_end_date',
    'admission_date',
    'discharge_date',
    'paid_date'
    )) as p
order by 1