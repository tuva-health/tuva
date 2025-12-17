{{ config(
     enabled = var('provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

-- Staging view of core medical claims used for provider attribution.
-- Provides the subset of fields needed by attribution, sourced only from core.

select
    claim_id
  , claim_line_number
  , person_id
  , claim_start_date
  , claim_end_date
  , allowed_amount
  , paid_amount
  , rendering_id as rendering_npi
  , hcpcs_code
  , data_source
  , encounter_id
from {{ ref('core__medical_claim') }}
