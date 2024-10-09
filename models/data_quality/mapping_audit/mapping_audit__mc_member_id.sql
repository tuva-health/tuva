{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a member_id
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] member_id_always_populated ('Yes' or 'No')
--    [b] member_id_unique ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  member_id
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),

member_id_not_always_populated as (
select distinct claim_id
from all_rows
where member_id is null
),

member_id_not_unique as (
select
  claim_id,
  count(distinct member_id) as member_id_count
from all_rows
group by claim_id
having member_id_count > 1
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as member_id_always_populated,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as member_id_unique

from all_claim_ids aa

left join member_id_not_always_populated bb
on aa.claim_id = bb.claim_id

left join member_id_not_unique cc
on aa.claim_id = cc.claim_id
)


select *
from final_table
