{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}


SELECT
  patient_id
, claim_id
, claim_line_number
, claim_start_date
, claim_end_date
, paid_amount
, allowed_amount
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__medical_claim') }}