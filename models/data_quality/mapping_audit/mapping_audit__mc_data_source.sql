{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a data_source
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] data_source_always_populated ('Yes' or 'No')
--    [b] data_source_unique ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  data_source
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),

data_source_not_always_populated as (
select distinct claim_id
from all_rows
where data_source is null
),

data_source_not_unique as (
select
  claim_id,
  count(distinct data_source) as data_source_count
from all_rows
group by claim_id
having data_source_count > 1
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as data_source_always_populated,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as data_source_unique

from all_claim_ids aa

left join data_source_not_always_populated bb
on aa.claim_id = bb.claim_id

left join data_source_not_unique cc
on aa.claim_id = cc.claim_id
)


select *
from final_table
