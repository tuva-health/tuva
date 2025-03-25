{{ config(
     enabled = var('ccsr_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

select
    encounter_id
    , claim_id
    , person_id
    , normalized_code
    , condition_rank
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__condition') }}
where normalized_code_type = 'icd-10-cm'
