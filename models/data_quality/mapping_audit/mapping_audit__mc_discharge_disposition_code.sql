{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a discharge_disposition_code
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] discharge_disposition_code_correct_length ('Yes' or 'No')
--    [b] discharge_disposition_code_unique ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  discharge_disposition_code
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),


discharge_disposition_code_not_correct_length as (
select
  distinct claim_id
from all_rows
where discharge_disposition_code is not null and
length(discharge_disposition_code) <> 2
),


discharge_disposition_code_not_unique as (
select
  claim_id,
  count(distinct discharge_disposition_code) as discharge_disposition_code_count
from all_rows
group by claim_id
having discharge_disposition_code_count > 1
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as discharge_disposition_code_unique,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as discharge_disposition_code_correct_length


from all_claim_ids aa

left join discharge_disposition_code_not_unique bb
on aa.claim_id = bb.claim_id

left join discharge_disposition_code_not_correct_length cc
on aa.claim_id = cc.claim_id
)


select *
from final_table
