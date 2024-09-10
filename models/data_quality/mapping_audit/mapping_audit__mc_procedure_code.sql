{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a procedure_code
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    procedure_code_type_unique ('Yes' or 'No')
--         'Yes' --> means that every procedure code that is populated on
--                   the claim has a unique value across all claim lines)
--         'No'  --> means that there is at least one procedure code that has
--                   different values on different claim lines

with all_rows as (
select
  claim_id,
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


procedure_code_not_unique as (
select
  claim_id,
  count(distinct procedure_code_1) as procedure_code_1_count,
  count(distinct procedure_code_2) as procedure_code_2_count,
  count(distinct procedure_code_3) as procedure_code_3_count,    
  count(distinct procedure_code_4) as procedure_code_4_count,
  count(distinct procedure_code_5) as procedure_code_5_count,    
  count(distinct procedure_code_6) as procedure_code_6_count,
  count(distinct procedure_code_7) as procedure_code_7_count,    
  count(distinct procedure_code_8) as procedure_code_8_count,
  count(distinct procedure_code_9) as procedure_code_9_count,    
  count(distinct procedure_code_10) as procedure_code_10_count,
  count(distinct procedure_code_11) as procedure_code_11_count,    
  count(distinct procedure_code_12) as procedure_code_12_count,
  count(distinct procedure_code_13) as procedure_code_13_count,    
  count(distinct procedure_code_14) as procedure_code_14_count,
  count(distinct procedure_code_15) as procedure_code_15_count,    
  count(distinct procedure_code_16) as procedure_code_16_count,
  count(distinct procedure_code_17) as procedure_code_17_count,    
  count(distinct procedure_code_18) as procedure_code_18_count,
  count(distinct procedure_code_19) as procedure_code_19_count,    
  count(distinct procedure_code_20) as procedure_code_20_count,
  count(distinct procedure_code_21) as procedure_code_21_count,    
  count(distinct procedure_code_22) as procedure_code_22_count,
  count(distinct procedure_code_23) as procedure_code_23_count,    
  count(distinct procedure_code_24) as procedure_code_24_count,
  count(distinct procedure_code_25) as procedure_code_25_count

from all_rows
group by claim_id
having
  (procedure_code_1_count > 1) or
  (procedure_code_2_count > 1) or  
  (procedure_code_3_count > 1) or
  (procedure_code_4_count > 1) or  
  (procedure_code_5_count > 1) or
  (procedure_code_6_count > 1) or  
  (procedure_code_7_count > 1) or
  (procedure_code_8_count > 1) or 
  (procedure_code_9_count > 1) or
  (procedure_code_10_count > 1) or  
  (procedure_code_11_count > 1) or
  (procedure_code_12_count > 1) or  
  (procedure_code_13_count > 1) or
  (procedure_code_14_count > 1) or  
  (procedure_code_15_count > 1) or
  (procedure_code_16_count > 1) or 
  (procedure_code_17_count > 1) or
  (procedure_code_18_count > 1) or  
  (procedure_code_19_count > 1) or
  (procedure_code_20_count > 1) or  
  (procedure_code_21_count > 1) or
  (procedure_code_22_count > 1) or  
  (procedure_code_23_count > 1) or
  (procedure_code_24_count > 1) or 
  (procedure_code_25_count > 1) 
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as procedure_code_unique


from all_claim_ids aa

left join procedure_code_not_unique bb
on aa.claim_id = bb.claim_id
)


select *
from final_table
