
{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with month_start_and_end_dates as (
  select
    {{ dbt.concat(["year",
                  dbt.right(dbt.concat(["'0'", "month"]), 2)]) }} as year_month
    , min(full_date) as month_start_date
    , max(full_date) as month_end_date
  from {{ ref('reference_data__calendar')}}
  group by year, month, year_month
)

, final as (
select 
    year_month
    , count(claim_id) AS claim_volume
    , sum(round(paid_amount, 2)) AS paid_amount
from {{ref('medical_claim')}} a
inner join 
    month_start_and_end_dates b
        on a.claim_start_date <= b.month_end_date
        and a.claim_start_date >= b.month_start_date
group by 
    year_month 
) 

select 
    year_month 
    , claim_volume 
    , paid_amount 
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from 
final 