{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a diagnosis_code_type
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] diagnosis_code_type_needed ('Yes' or 'No')
--    [b] diagnosis_code_type_valid ('Yes' or 'No')
--    [c] diagnosis_code_type_unique ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  diagnosis_code_type,
  diagnosis_code_1,
  diagnosis_code_2,
  diagnosis_code_3,
  diagnosis_code_4,
  diagnosis_code_5,
  diagnosis_code_6,
  diagnosis_code_7,
  diagnosis_code_8,
  diagnosis_code_9,
  diagnosis_code_10,
  diagnosis_code_11,
  diagnosis_code_12,
  diagnosis_code_13,
  diagnosis_code_14,
  diagnosis_code_15,
  diagnosis_code_16,
  diagnosis_code_17,
  diagnosis_code_18,
  diagnosis_code_19,
  diagnosis_code_20,
  diagnosis_code_21,
  diagnosis_code_22,
  diagnosis_code_23,
  diagnosis_code_24,
  diagnosis_code_25
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),


diagnosis_code_type_needed as (
select distinct claim_id
from all_rows
where
  diagnosis_code_1 is not null or
  diagnosis_code_2 is not null or
  diagnosis_code_3 is not null or
  diagnosis_code_4 is not null or
  diagnosis_code_5 is not null or
  diagnosis_code_6 is not null or
  diagnosis_code_7 is not null or
  diagnosis_code_8 is not null or
  diagnosis_code_9 is not null or
  diagnosis_code_10 is not null or
  diagnosis_code_11 is not null or
  diagnosis_code_12 is not null or
  diagnosis_code_13 is not null or
  diagnosis_code_14 is not null or
  diagnosis_code_15 is not null or
  diagnosis_code_16 is not null or
  diagnosis_code_17 is not null or
  diagnosis_code_18 is not null or
  diagnosis_code_19 is not null or
  diagnosis_code_20 is not null or
  diagnosis_code_21 is not null or
  diagnosis_code_22 is not null or
  diagnosis_code_23 is not null or
  diagnosis_code_24 is not null or
  diagnosis_code_25 is not null
),


diagnosis_code_type_not_valid as (
select distinct claim_id
from all_rows
where diagnosis_code_type not in ('icd-9-cm', 'icd-10-cm') 
),



diagnosis_code_type_not_unique as (
select
  claim_id,
  count(distinct diagnosis_code_type) as diagnosis_code_type_count
from all_rows
group by claim_id
having diagnosis_code_type_count > 1
),



final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'Yes'
    when bb.claim_id is null then 'No'
  end as diagnosis_code_type_needed,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as diagnosis_code_type_valid,

  case
    when dd.claim_id is not null then 'No'
    when dd.claim_id is null then 'Yes'
  end as diagnosis_code_type_unique


from all_claim_ids aa

left join diagnosis_code_type_needed bb
on aa.claim_id = bb.claim_id

left join diagnosis_code_type_not_valid cc
on aa.claim_id = cc.claim_id

left join diagnosis_code_type_not_unique dd
on aa.claim_id = dd.claim_id
)


select *
from final_table
