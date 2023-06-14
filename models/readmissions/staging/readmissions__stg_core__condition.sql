{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  encounter_id
, code
, diagnosis_rank
, code_type
, claim_id
, '{{ var('last_update')}}' as last_update
from {{ ref('core__condition') }}
