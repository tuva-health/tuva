{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a revenue_center_code
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] revenue_center_code_correct_length ('Yes' or 'No')
--    [b] revenue_center_code_unique ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  revenue_center_code
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),


revenue_center_code_not_correct_length as (
select
  distinct claim_id
from all_rows
where revenue_center_code is not null and
length(revenue_center_code) <> 4
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as revenue_center_code_correct_length


from all_claim_ids aa

left join revenue_center_code_not_correct_length bb
on aa.claim_id = bb.claim_id
)


select *
from final_table
