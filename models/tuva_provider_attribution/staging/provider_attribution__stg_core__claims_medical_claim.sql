{{ config(
     enabled = var('provider_attribution_enabled', var('claims_enabled', var('tuva_marts_enabled', True))) | as_bool
   )
}}

select
    claim_id
  , claim_line_number
  , data_source
  , encounter_id
from {{ ref('core__stg_claims_medical_claim') }}
