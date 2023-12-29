

-- This dbt model has these columns:
--     claim_id
--     distinct_rev_code_count (positive integer: count of distinct R&B rev codes this claim has)
--     has_a_valid_rev_code (0 or 1 flag, indicates if the claim has at least one valid rev code)
--     has_an_invalid_rev_code (0 or 1 flag, indicates if the
--                                           claim has at least one invalid rev code)
--     basic (0 or 1 flag, indicates if the claim has at least one basic rev code)
--     hospice (0 or 1 flag, indicates if the claim has at least one hospice rev code)
--     loa (0 or 1 flag, indicates if the claim has at least one leave of absence rev code)
--     behavioral (0 or 1 flag indicates if the claim has at least one behavioral rev code)


-- This dbt model has one row per claim_id that is in the
-- core.medical_claim table that has a
-- room & board rev code (i.e. a rev code that is
-- between '0100' and '0219' or between '1000' and '1002').
-- Note that some of the codes in this range might not be from terminology
-- so they might be invalid. That is why we have the 'has_valid_rev_code' flag.



with group_by_claim_id as (
select
  claim_id,
  count(distinct revenue_center_code) as distinct_rev_code_count,
  
  case
    when max(valid_rev_code) = 1 then 1
    else 0
  end as has_a_valid_rev_code,

  case
    when min(valid_rev_code) = 0 then 1
    else 0
  end as has_an_invalid_rev_code,

  max(basic) as basic,
  max(hospice) as hospice,
  max(loa) as loa,
  max(behavioral) as behavioral
  
from {{ ref('all_line_level_room_and_board_rev_codes') }}
group by claim_id
)

select *
from group_by_claim_id
