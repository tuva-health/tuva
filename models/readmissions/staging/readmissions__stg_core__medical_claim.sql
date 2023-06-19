{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
   )
}}


SELECT
  claim_id
, claim_type
, encounter_id
, encounter_type
, '{{ var('last_update')}}' as last_update
from {{ ref('core__medical_claim') }}