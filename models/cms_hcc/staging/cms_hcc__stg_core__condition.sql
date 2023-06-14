{{ config(
     enabled = var('cms_hcc_enabled',var('tuva_marts_enabled',True))
   )
}}
select
      claim_id
    , patient_id
    , code_type
    , code
    , '{{ var('last_update')}}' as last_update
from {{ ref('core__condition') }}