{{ config(
     enabled = var('tuva_provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select
    claim_id
  , claim_line_number
  , data_source
  , encounter_id
from {{ ref('core__stg_claims_medical_claim') }}
