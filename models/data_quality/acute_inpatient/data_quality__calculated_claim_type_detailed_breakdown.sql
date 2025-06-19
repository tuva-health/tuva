{{ config(
    enabled = var('claims_enabled', False)
) }}

with total_claims_count as (
      select
            count(*) as total_claims_count
      from {{ ref('data_quality__claim_type') }}
)

select 
      calculated_claim_type
    , count(*) as total_claims
    , round(
          count(*) * 100.0 / total_claims_count.total_claims_count, 1
      ) as percent_of_claims
    , has_institutional_fields
    , has_valid_institutional_fields
    , has_professional_fields
    , has_valid_professional_fields
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__claim_type') }}
cross join total_claims_count
group by 
      calculated_claim_type
    , has_institutional_fields
    , has_valid_institutional_fields
    , has_professional_fields
    , has_valid_professional_fields
    , total_claims_count.total_claims_count
