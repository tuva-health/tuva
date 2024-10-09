{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to check that every claim_id has values of claim_line_number
-- across all claim lines that are sequential positive integers
-- starting at 1.
-- This will always be the case if:
--    [a] The minimum claim_line_number value for a claim_id is 1.
--    [b] The maximum claim_line_number for a claim_id is equal to the number of
--        lines for that claim_id.
--    [c] All values of claim_line_number for a claim_id are different for every
--        claim line.

-- NB: we have modified this file to only check for [c] because
--     we have decided that we only want to check that the claim_line_number
--     is unique across different lines within a claim, not that it is
--     a sequential positive integer.


-- All distinct claim_id, claim_line_number values:
with all_rows as (
select
  claim_id,
  claim_line_number
from {{ ref('pharmacy_claim') }}
where claim_id is not null
),


all_claim_ids as (
select distinct claim_id
from all_rows
),

-- All claim_id values for which the minimum claim_line_number is
-- not 1:
min_is_not_one as (
select
  claim_id,
  min(claim_line_number) as min_claim_line_number
from all_rows
group by claim_id
having min_claim_line_number <> 1
),

-- All claim_id values for which the max claim_line_number is
-- not equal to the number of claim_lines:
max_is_not_equal_to_number_of_lines as (
select
  claim_id,
  max(claim_line_number) as max_claim_line_number,
  count(*) as number_of_lines
from all_rows
group by claim_id
having max_claim_line_number <> number_of_lines
),


-- All claim_id values that have a repeated claim_line_number value on two
-- different claim lines:
repeated_claim_line_number_values as (
select distinct claim_id
  from (
      select
        claim_id,
        claim_line_number,
        count(*) as count_of_claim_line_number
      from all_rows
      group by claim_id, claim_line_number
      having count_of_claim_line_number > 1
  )    
),

problematic_claims as (
select distinct claim_id
from all_claim_ids
where
--  claim_id in (select distinct claim_id from min_is_not_one) or
--  claim_id in (select distinct claim_id from max_is_not_equal_to_number_of_lines) or
  claim_id in (select distinct claim_id from repeated_claim_line_number_values)
),


final_table as (
select
  aa.claim_id,
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
--  end as sequential_claim_line_number
  end as claim_line_number_unique
from all_claim_ids aa
left join problematic_claims bb
on aa.claim_id = bb.claim_id
)


select *
from final_table
