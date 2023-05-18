{{ config(
     enabled = var('claims_date_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  coalesce(a.year_month,b.year_month,c.year_month) as year_month
, c.member_months
, a."'claim_start_date'" as claim_start_date
, a."'claim_end_date'" as claim_end_date
, a."'admission_date'" as admission_date
, a."'discharge_date'" as discharge_date
, a."'paid_date'" as medical_paid_date
, b."'dispensing_date'" as dispensing_date
, b."'paid_date'" as rx_paid_date
from {{ ref('claims_date_profiling__medical_claim_dates') }} a
full join {{ ref('claims_date_profiling__pharmacy_claim_dates') }} b
    on a.year_month = b.year_month
full join {{ ref('claims_date_profiling__member_months_count') }} c
    on a.year_month = c.year_month

union all

select 
  'Duplicate' as year_month
, null as member_months
, c.*
from {{ ref('claims_date_profiling__duplicate_dates') }} c
order by 1