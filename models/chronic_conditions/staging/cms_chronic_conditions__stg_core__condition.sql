{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
      claim_id
    , person_id
    , recorded_date
    , normalized_code_type
    , normalized_code
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__condition') }}
where claim_id is not null
