
-- This dbt model shows how many claims there are with each existing
-- combination of the 4 types of room & board rev codes (basic, hospice,
-- loa, behavioral).
-- It has these columns:
--   basic (0 or 1 flag, indicates if the claim has at least one 'basic' room & board rev code)
--   hospice (0 or 1 flag, indicates if the claim has at least one 'hospice'
--            room & board rev code)
--   loa (0 or 1 flag, indicates if the claim has at least one 'leave of absense'
--        room & board rev code)
--   behavioral (0 or 1 flag, indicates if the claim has at least one 'behavioral'
--               room & board rev code)
--   claim_count (count of claims that have the particular combination of flags
--                indicated by the first 4 columns)
--   claim_percent (percent of claims that have the particular combination
--                  of flags indicated by the first 4 columns)

-- This model can give us intuition about which of the 4 categories of
-- rev codes ('basic', 'hospice', 'leave of absence', 'behavioral')
-- between '0100' and '0219' or between '1000' and '1002' most frequently
-- occur in claims.



with group_by_types_of_rb_claims as (
select
  basic,
  hospice,
  loa,
  behavioral,
  count(*) as claim_count,
  count(*) * 100.0 / (select count(*) from {{ ref('rb_claims') }} ) as claim_percent

from {{ ref('rb_claims') }}
group by basic, hospice, loa, behavioral
)


select *
from group_by_types_of_rb_claims
