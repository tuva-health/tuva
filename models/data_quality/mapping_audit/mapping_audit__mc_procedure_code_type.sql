{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a procedure_code_type
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] procedure_code_type_needed ('Yes' or 'No')
--    [b] procedure_code_type_valid ('Yes' or 'No')
--    [c] procedure_code_type_unique ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  procedure_code_type,
  procedure_code_1,
  procedure_code_2,
  procedure_code_3,
  procedure_code_4,
  procedure_code_5,
  procedure_code_6,
  procedure_code_7,
  procedure_code_8,
  procedure_code_9,
  procedure_code_10,
  procedure_code_11,
  procedure_code_12,
  procedure_code_13,
  procedure_code_14,
  procedure_code_15,
  procedure_code_16,
  procedure_code_17,
  procedure_code_18,
  procedure_code_19,
  procedure_code_20,
  procedure_code_21,
  procedure_code_22,
  procedure_code_23,
  procedure_code_24,
  procedure_code_25
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),


procedure_code_type_needed as (
select distinct claim_id
from all_rows
where
  procedure_code_1 is not null or
  procedure_code_2 is not null or
  procedure_code_3 is not null or
  procedure_code_4 is not null or
  procedure_code_5 is not null or
  procedure_code_6 is not null or
  procedure_code_7 is not null or
  procedure_code_8 is not null or
  procedure_code_9 is not null or
  procedure_code_10 is not null or
  procedure_code_11 is not null or
  procedure_code_12 is not null or
  procedure_code_13 is not null or
  procedure_code_14 is not null or
  procedure_code_15 is not null or
  procedure_code_16 is not null or
  procedure_code_17 is not null or
  procedure_code_18 is not null or
  procedure_code_19 is not null or
  procedure_code_20 is not null or
  procedure_code_21 is not null or
  procedure_code_22 is not null or
  procedure_code_23 is not null or
  procedure_code_24 is not null or
  procedure_code_25 is not null
),


procedure_code_type_not_valid as (
select distinct claim_id
from all_rows
where procedure_code_type not in ('icd-9-pcs', 'icd-10-pcs') 
),



procedure_code_type_not_unique as (
select
  claim_id,
  count(distinct procedure_code_type) as procedure_code_type_count
from all_rows
group by claim_id
having procedure_code_type_count > 1
),



final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'Yes'
    when bb.claim_id is null then 'No'
  end as procedure_code_type_needed,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as procedure_code_type_valid,

  case
    when dd.claim_id is not null then 'No'
    when dd.claim_id is null then 'Yes'
  end as procedure_code_type_unique


from all_claim_ids aa

left join procedure_code_type_needed bb
on aa.claim_id = bb.claim_id

left join procedure_code_type_not_valid cc
on aa.claim_id = cc.claim_id

left join procedure_code_type_not_unique dd
on aa.claim_id = dd.claim_id
)


select *
from final_table
