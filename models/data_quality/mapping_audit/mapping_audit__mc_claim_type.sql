{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a claim_type
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] claim_type_always_populated ('Yes' or 'No')
--    [b] claim_type_unique ('Yes' or 'No')
--    [c] claim_type_valid_values ('Yes' or 'No')
--        (Valid values for claim_type are 'institutional', 'professional', or 'undetermined')


with all_rows as (
select
  claim_id,
  claim_type
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),

claim_type_not_always_populated as (
select distinct claim_id
from all_rows
where claim_type is null
),

claim_type_not_unique as (
select
  claim_id,
  count(distinct claim_type) as claim_type_count
from all_rows
group by claim_id
having claim_type_count > 1
),

claim_type_invalid_values as (
select distinct claim_id
from all_rows
where claim_type not in ('institutional','professional','undetermined')
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as claim_type_always_populated,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as claim_type_unique,

  case
    when dd.claim_id is not null then 'No'
    when dd.claim_id is null then 'Yes'
  end as claim_type_valid_values

from all_claim_ids aa

left join claim_type_not_always_populated bb
on aa.claim_id = bb.claim_id

left join claim_type_not_unique cc
on aa.claim_id = cc.claim_id

left join claim_type_invalid_values dd
on aa.claim_id = dd.claim_id
)


select *
from final_table
