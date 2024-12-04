{{ config(
    enabled = var('claims_enabled', False)
) }}

select
    distinct_rev_code_count
  , count(*) as claim_count
  , count(*) * 100.0 / (
        select count(*)
        from {{ ref('rb_claims') }}
    ) as claim_percent
from {{ ref('rb_claims') }}
group by distinct_rev_code_count
