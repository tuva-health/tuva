{{ config(
     enabled = var('cms_hcc_enabled',var('tuva_marts_enabled',True))
   )
}}
select
      claim_id
    , patient_id
    , normalized_code_type as code_type
    , normalized_code as code
    , '{{ var('last_update')}}' as last_update
from {{ ref('core__condition') }}