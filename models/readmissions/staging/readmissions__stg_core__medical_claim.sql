{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


SELECT
  claim_id
, claim_type
, encounter_id
, encounter_type
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__medical_claim') }}