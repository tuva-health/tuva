{{ config(
     enabled = var('claims_preprocessing',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
  coalesce(a.year_month,b.year_month,c.year_month) as year_month
, a.claim_start_date as claim_start_date
, a.claim_end_date as claim_end_date
, a.admission_date as admission_date
, a.discharge_date as discharge_date
, a.paid_date as medical_paid_date
, b.dispensing_date as dispensing_date
, b.paid_date as rx_paid_date
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('claims_date_profiling__medical_claim_dates') }} a
full join {{ ref('claims_date_profiling__pharmacy_claim_dates') }} b
    on a.year_month = b.year_month

union all

select 
  'Duplicate' as year_month
, c.claim_start_date
, c.claim_end_date
, c.admission_date
, c.discharge_date
, c.med_paid_date as medical_paid_date
, c.dispensing_date
, c.rx_paid_date
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('claims_date_profiling__duplicate_dates') }} c
order by 1