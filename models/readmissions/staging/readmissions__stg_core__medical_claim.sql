{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
   )
}}


SELECT
  claim_id
, claim_type
, encounter_id
, encounter_type
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__medical_claim') }}