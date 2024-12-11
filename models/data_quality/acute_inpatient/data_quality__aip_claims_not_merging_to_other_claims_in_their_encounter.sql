{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct 
      claim_id
      , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__aip_multiple_claim_encounters') }}
where claim_id not in (
    
    select distinct 
          claim_id_a
    from {{ ref('data_quality__aip_claims_that_merge_to_a_different_claim_within_same_encounter') }}

)
