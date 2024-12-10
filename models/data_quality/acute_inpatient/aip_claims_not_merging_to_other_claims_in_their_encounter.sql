{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct 
      claim_id
from {{ ref('aip_multiple_claim_encounters') }}
where claim_id not in (
    
    select distinct 
          claim_id_a
    from {{ ref('aip_claims_that_merge_to_a_different_claim_within_same_encounter') }}

)
