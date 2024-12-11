{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      revenue_center_code as invalid_rev
    , count(distinct claim_id) as number_of_claims
    , count(distinct claim_id) * 100.0 /
        (
          select total_claims
          from {{ ref('data_quality__calculated_claim_type_percentages') }}
          where calculated_claim_type = 'institutional'
        )
        as percent_of_institutional_claims
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__rev_all') }}
where valid_revenue_center_code = 0
group by revenue_center_code
