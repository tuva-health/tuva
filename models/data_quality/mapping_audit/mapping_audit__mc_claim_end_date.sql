{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a claim_end_date
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] claim_end_date_always_populated ('Yes' or 'No')
--    [b] claim_end_date_unique ('Yes' or 'No')

-- NB:
-- We edit this file to remove the requirement [a], since
-- there might be datasets where claim_end_date is not available
-- and we therefore cannot map it to every claim.

with all_rows as (
select
  claim_id,
  claim_end_date
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),

claim_end_date_not_always_populated as (
select distinct claim_id
from all_rows
where claim_end_date is null
),

claim_end_date_not_unique as (
select
  claim_id,
  count(distinct claim_end_date) as claim_end_date_count
from all_rows
group by claim_id
having claim_end_date_count > 1
),


final_table as (
select
  aa.claim_id,
  
  -- case
  --   when bb.claim_id is not null then 'No'
  --   when bb.claim_id is null then 'Yes'
  -- end as claim_end_date_always_populated,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as claim_end_date_unique

from all_claim_ids aa

left join claim_end_date_not_always_populated bb
on aa.claim_id = bb.claim_id

left join claim_end_date_not_unique cc
on aa.claim_id = cc.claim_id
)


select *
from final_table
