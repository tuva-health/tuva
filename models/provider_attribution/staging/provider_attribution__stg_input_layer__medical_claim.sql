{{ config(
     enabled = var('provider_attribution_enabled', var('tuva_marts_enabled', True)) | as_bool
   )
}}

select
    claim_id
  , claim_line_number
  , person_id
  , claim_start_date
  , claim_end_date
  , allowed_amount
  , paid_amount
  , rendering_npi
  , hcpcs_code
  , data_source
from {{ ref('input_layer__medical_claim') }}
