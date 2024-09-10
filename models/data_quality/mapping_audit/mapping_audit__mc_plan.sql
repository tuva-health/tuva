{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a plan
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] plan_always_populated ('Yes' or 'No')
--    [b] plan_unique ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  plan
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),

plan_not_always_populated as (
select distinct claim_id
from all_rows
where plan is null
),

plan_not_unique as (
select
  claim_id,
  count(distinct plan) as plan_count
from all_rows
group by claim_id
having plan_count > 1
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as plan_always_populated,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as plan_unique

from all_claim_ids aa

left join plan_not_always_populated bb
on aa.claim_id = bb.claim_id

left join plan_not_unique cc
on aa.claim_id = cc.claim_id
)


select *
from final_table
