{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      place_of_service_code as invalid_pos
    , count(distinct claim_id) as number_of_claims
    , count(distinct claim_id) * 100.0 /
        (
          select total_claims
          from {{ ref('data_quality__calculated_claim_type_percentages') }}
          where calculated_claim_type = 'professional'
        )
        as percent_of_professional_claims
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__pos_all') }}
where valid_place_of_service_code = 0
group by place_of_service_code

