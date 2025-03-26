{{ config(
     enabled = var('ccsr_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

select
      encounter_id
    , claim_id
    , person_id
    , normalized_code
    , code_description
    , ccsr_category
    , ccsr_category_description
    , ccsr_parent_category
    , parent_category_description
    , body_system
    , {{ var('dxccsr_version') }} as dxccsr_version
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ccsr__long_condition_category') }}
where is_{{ var('record_type', 'ip') }}_default_category = 1
and condition_rank = 1
