{{ config(
     enabled = (var('provider_attribution_enabled', False) and var('claims_enabled', False))
   )
}}

select
    claim_id
  , claim_line_number
  , data_source
  , encounter_id
from {{ ref('core__stg_claims_medical_claim') }}
