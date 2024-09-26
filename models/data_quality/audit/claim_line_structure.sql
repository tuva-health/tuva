{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}


select
  claim_id,
  claim_line_number,
  data_source

from {{ ref('medical_claim') }}
