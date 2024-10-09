{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a ms_drg_code
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] ms_drg_code_correct_length ('Yes' or 'No')
--    [b] ms_drg_code_unique ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  ms_drg_code
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),


ms_drg_code_not_correct_length as (
select
  distinct claim_id
from all_rows
where ms_drg_code is not null and
length(ms_drg_code) <> 3
),


ms_drg_code_not_unique as (
select
  claim_id,
  count(distinct ms_drg_code) as ms_drg_code_count
from all_rows
group by claim_id
having ms_drg_code_count > 1
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as ms_drg_code_unique,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as ms_drg_code_correct_length


from all_claim_ids aa

left join ms_drg_code_not_unique bb
on aa.claim_id = bb.claim_id

left join ms_drg_code_not_correct_length cc
on aa.claim_id = cc.claim_id
)


select *
from final_table
