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
from {{ ref('core__condition') }}
