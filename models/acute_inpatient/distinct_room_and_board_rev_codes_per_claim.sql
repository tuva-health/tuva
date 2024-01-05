
-- This dbt model shows us how many claims have each occurring count of
-- distinct room & board rev codes.
-- It has these columns:
--    distinct_rev_code_count (positive integer representing
--                             the number of distinct room & board rev codes a claim has)
--    claim_count (positive integer representing the number of distinct
--                 claims that have the rev code count given by the first column)
--    claim_percent (claim_count from the previous column * 100, divided by
--                   the number of claims that have at least one rev code
--                   between '0100' and '0219' or between '1000' and '1002')

--  This model is useful to see how common it is for claims to have different
--  numbers of distinct room and board rev codes.


select
  distinct_rev_code_count,
  count(*) as claim_count,
  count(*) * 100.0 / (select count(*) from {{ ref('rb_claims') }} ) as claim_percent

from {{ ref('rb_claims') }}
group by distinct_rev_code_count

