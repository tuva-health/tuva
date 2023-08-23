{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}


SELECT
  patient_id
, dispensing_date
, paid_amount
, allowed_amount
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('pharmacy_claim') }}