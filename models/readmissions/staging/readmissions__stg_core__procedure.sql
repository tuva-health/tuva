{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
  encounter_id
, normalized_code
, normalized_code_type
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__procedure') }}
