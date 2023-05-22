{{ config(
     enabled = var('claims_date_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

with rx_claims as (
select
  min(dispensing_date) as dispensing_date
, min(paid_date) as paid_date
, claim_id
from {{ ref('pharmacy_claim') }} 
group by claim_id
)

, rx_transform as (
select
  date_part(year,dispensing_date) || lpad(date_part(month,dispensing_date),2,0) as dispensing_date
, date_part(year,paid_date) || lpad(date_part(month,paid_date),2,0) as paid_date
, claim_id
from rx_claims
)

, rx_pivot_prep as (
select
  dispensing_date as year_month
, 'dispensing_date' as date_type
, count(distinct claim_id) as cnt
from rx_transform 
group by 1,2

union all

select
  paid_date as year_month
, 'paid_date' as date_type
, count(distinct claim_id) as cnt
from rx_transform 
group by 1,2
)

select
year_month
, {{ dbt_utils.pivot(
        column='date_type'
    , values=['dispensing_date','paid_date']
    , agg='sum'
    , quote_identifiers=false
    ) }}
from rx_pivot_prep
group by year_month