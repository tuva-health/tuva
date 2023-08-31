{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
      claim_id
    , patient_id
    , claim_start_date
    , ms_drg_code
    , data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim') }}