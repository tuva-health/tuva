{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

with pharmacy_claim as (
select claim_id
,max(case when paid_date is null then 1 else 0 end) as missing_paid_date
,max(case when dispensing_date is null then 1 else 0 end) as missing_dispensing_date
from {{ ref('pharmacy_claim')}}
group by claim_id
)

select 'missing paid_date'
,sum(missing_paid_date) as result_count
from pharmacy_claim

union all

select 'missing dispensing_date'
,sum(missing_dispensing_date) as result_count
from pharmacy_claim
