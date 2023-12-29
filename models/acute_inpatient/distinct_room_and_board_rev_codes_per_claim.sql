
-- This dbt model has these columns:
--     distinct_rev_code_count
--     claim_count
--     claim_percent

-- This model is useful to see how common it is for claims to
-- have different numbers of distinct room and board rev codes.


select
  distinct_rev_code_count,
  count(*) as claim_count,
  count(*) * 100.0 / (select count(*) from {{ ref('rb_claims') }} ) as claim_percent

from {{ ref('rb_claims') }}
group by distinct_rev_code_count

