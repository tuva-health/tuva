{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',True))
   )
}}

select 
  year_month
, count(1) as member_months
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('member_months__member_months') }}
group by 1