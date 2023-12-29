

-- This dbt model has these columns:
--     basic
--     hospice
--     loa
--     behavioral
--     claim_count
--     claim_percent

-- The goal is to show how many claims that have room & board rev codes
-- (rev codes between '0100' and '0219' or between '1000' and '1002')
-- have at least one of the 4 types of room & board rev code types:
-- basic, hospice, loa, behavioral.


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
