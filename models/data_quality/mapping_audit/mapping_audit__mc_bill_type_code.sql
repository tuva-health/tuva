{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a bill_type_code
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] bill_type_code_correct_length ('Yes' or 'No')
--    [b] bill_type_code_unique ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  bill_type_code
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),


bill_type_code_not_correct_length as (
select
  distinct claim_id
from all_rows
where bill_type_code is not null and
length(bill_type_code) <> 3
),


bill_type_code_not_unique as (
select
  claim_id,
  count(distinct bill_type_code) as bill_type_code_count
from all_rows
group by claim_id
having bill_type_code_count > 1
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as bill_type_code_unique,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as bill_type_code_correct_length


from all_claim_ids aa

left join bill_type_code_not_unique bb
on aa.claim_id = bb.claim_id

left join bill_type_code_not_correct_length cc
on aa.claim_id = cc.claim_id
)


select *
from final_table
