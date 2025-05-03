{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}
select
      claim_id
    , person_id
    , recorded_date
    , condition_type
    , normalized_code_type as code_type
    , normalized_code as code
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__condition') }}
