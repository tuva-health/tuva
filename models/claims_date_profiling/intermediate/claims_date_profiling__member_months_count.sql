{{ config(
     enabled = var('claims_date_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

select 
  year_month
, count(1) as member_months
from {{ ref('member_months') }}
group by 1
order by 1