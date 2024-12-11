{{ config(
    enabled = var('claims_enabled', False)
) }}

select
    distinct_rev_code_count
  , count(*) as claim_count
  , count(*) * 100.0 / (
        select count(*)
        from {{ ref('data_quality__rb_claims') }}
    ) as claim_percent
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__rb_claims') }}
group by distinct_rev_code_count
