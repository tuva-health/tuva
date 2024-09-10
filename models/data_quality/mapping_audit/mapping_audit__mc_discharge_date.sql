{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a discharge_date
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] discharge_date_unique ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  discharge_date
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),


discharge_date_not_unique as (
select
  claim_id,
  count(distinct discharge_date) as discharge_date_count
from all_rows
group by claim_id
having discharge_date_count > 1
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as discharge_date_unique

from all_claim_ids aa

left join discharge_date_not_unique bb
on aa.claim_id = bb.claim_id
)


select *
from final_table
