{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a diagnosis_code
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    diagnosis_code_type_unique ('Yes' or 'No')
--         'Yes' --> means that every diagnosis code that is populated on
--                   the claim has a unique value across all claim lines)
--         'No'  --> means that there is at least one diagnosis code that has
--                   different values on different claim lines

with all_rows as (
select
  claim_id,
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


diagnosis_code_not_unique as (
select
  claim_id,
  count(distinct diagnosis_code_1) as diagnosis_code_1_count,
  count(distinct diagnosis_code_2) as diagnosis_code_2_count,
  count(distinct diagnosis_code_3) as diagnosis_code_3_count,    
  count(distinct diagnosis_code_4) as diagnosis_code_4_count,
  count(distinct diagnosis_code_5) as diagnosis_code_5_count,    
  count(distinct diagnosis_code_6) as diagnosis_code_6_count,
  count(distinct diagnosis_code_7) as diagnosis_code_7_count,    
  count(distinct diagnosis_code_8) as diagnosis_code_8_count,
  count(distinct diagnosis_code_9) as diagnosis_code_9_count,    
  count(distinct diagnosis_code_10) as diagnosis_code_10_count,
  count(distinct diagnosis_code_11) as diagnosis_code_11_count,    
  count(distinct diagnosis_code_12) as diagnosis_code_12_count,
  count(distinct diagnosis_code_13) as diagnosis_code_13_count,    
  count(distinct diagnosis_code_14) as diagnosis_code_14_count,
  count(distinct diagnosis_code_15) as diagnosis_code_15_count,    
  count(distinct diagnosis_code_16) as diagnosis_code_16_count,
  count(distinct diagnosis_code_17) as diagnosis_code_17_count,    
  count(distinct diagnosis_code_18) as diagnosis_code_18_count,
  count(distinct diagnosis_code_19) as diagnosis_code_19_count,    
  count(distinct diagnosis_code_20) as diagnosis_code_20_count,
  count(distinct diagnosis_code_21) as diagnosis_code_21_count,    
  count(distinct diagnosis_code_22) as diagnosis_code_22_count,
  count(distinct diagnosis_code_23) as diagnosis_code_23_count,    
  count(distinct diagnosis_code_24) as diagnosis_code_24_count,
  count(distinct diagnosis_code_25) as diagnosis_code_25_count

from all_rows
group by claim_id
having
  (diagnosis_code_1_count > 1) or
  (diagnosis_code_2_count > 1) or  
  (diagnosis_code_3_count > 1) or
  (diagnosis_code_4_count > 1) or  
  (diagnosis_code_5_count > 1) or
  (diagnosis_code_6_count > 1) or  
  (diagnosis_code_7_count > 1) or
  (diagnosis_code_8_count > 1) or 
  (diagnosis_code_9_count > 1) or
  (diagnosis_code_10_count > 1) or  
  (diagnosis_code_11_count > 1) or
  (diagnosis_code_12_count > 1) or  
  (diagnosis_code_13_count > 1) or
  (diagnosis_code_14_count > 1) or  
  (diagnosis_code_15_count > 1) or
  (diagnosis_code_16_count > 1) or 
  (diagnosis_code_17_count > 1) or
  (diagnosis_code_18_count > 1) or  
  (diagnosis_code_19_count > 1) or
  (diagnosis_code_20_count > 1) or  
  (diagnosis_code_21_count > 1) or
  (diagnosis_code_22_count > 1) or  
  (diagnosis_code_23_count > 1) or
  (diagnosis_code_24_count > 1) or 
  (diagnosis_code_25_count > 1) 
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as diagnosis_code_unique


from all_claim_ids aa

left join diagnosis_code_not_unique bb
on aa.claim_id = bb.claim_id
)


select *
from final_table
