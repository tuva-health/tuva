{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a ndc_code
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] ndc_code_correct_length ('Yes' or 'No')
--    [b] ndc_code_always_populated ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  ndc_code
from {{ ref('pharmacy_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),


ndc_code_not_correct_length as (
select
  distinct claim_id
from all_rows
where ndc_code is not null and
length(ndc_code) <> 11
),

ndc_code_not_always_populated as (
select distinct claim_id
from all_rows
where ndc_code is null
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as ndc_code_correct_length,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as ndc_code_always_populated


from all_claim_ids aa

left join ndc_code_not_correct_length bb
on aa.claim_id = bb.claim_id

left join ndc_code_not_always_populated cc
on aa.claim_id = cc.claim_id
)


select *
from final_table
