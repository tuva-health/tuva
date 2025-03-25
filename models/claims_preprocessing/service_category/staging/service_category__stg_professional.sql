{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
    a.claim_id
  , a.claim_line_number
  , a.claim_line_id as claim_line_id
  , 'professional' as service_type
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as a
where a.claim_type = 'professional'
