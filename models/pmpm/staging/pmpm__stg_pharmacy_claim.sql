{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}


SELECT
  patient_id
, dispensing_date
, paid_amount
, allowed_amount
, '{{ var('last_update')}}' as last_update
from {{ ref('pharmacy_claim') }}