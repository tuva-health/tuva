{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
  encounter_id
, normalized_code
, condition_rank
, normalized_code_type
, claim_id
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__condition') }}
