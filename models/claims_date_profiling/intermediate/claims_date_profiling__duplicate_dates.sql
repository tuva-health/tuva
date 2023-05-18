{{ config(
     enabled = var('claims_date_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

with claim_start as (
select claim_id
from {{ ref('medical_claim') }} 
group by claim_id
having count(distinct claim_start_date) > 1
)

, claim_end as (
select claim_id
from {{ ref('medical_claim') }} 
group by claim_id
having count(distinct claim_end_date) > 1
)

, admission_date as (
select claim_id
from {{ ref('medical_claim') }} 
group by claim_id
having count(distinct admission_date) > 1
)

, discharge_date as (
select claim_id
from {{ ref('medical_claim') }} 
group by claim_id
having count(distinct discharge_date) > 1
)

, med_paid_date as (
select claim_id
from {{ ref('medical_claim') }} 
group by claim_id
having count(distinct paid_date) > 1
)

, dispensing_date as (
select claim_id
from {{ ref('pharmacy_claim') }} 
group by claim_id
having count(distinct dispensing_date) > 1
)

, rx_paid_date as (
select claim_id
from {{ ref('medical_claim') }} 
group by claim_id
having count(distinct paid_date) > 1
)

, combine as (
select 
  'claim_start_date' as date_type
, count(1) as cnt
from claim_start

union

select 
  'claim_end_date' as date_type
, count(1) as cnt
from claim_end

union

select 
  'admission_date' as date_type
, count(1) as cnt
from admission_date

union

select 
  'discharge_date' as date_type
, count(1) as cnt
from discharge_date

union

select 
  'med_paid_date' as date_type
, count(1) as cnt
from med_paid_date

union

select 
  'dispensing_date' as date_type
, count(1) as cnt
from dispensing_date

union

select 
  'rx_paid_date' as date_type
, count(1) as cnt
from rx_paid_date
)

select *
from combine
pivot (sum(cnt) for date_type in (
    'claim_start_date',
    'claim_end_date',
    'admission_date',
    'discharge_date',
    'med_paid_date',
    'dispensing_date',
    'rx_paid_date'
    )) as p
order by 1