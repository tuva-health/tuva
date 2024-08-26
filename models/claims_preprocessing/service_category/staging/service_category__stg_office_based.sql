{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct 
  a.claim_id
  , a.claim_line_number
  , a.claim_line_id
  , 'office based' as service_type
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
inner join  {{ ref('service_category__stg_professional') }} p on a.claim_id = p.claim_id 
and
a.claim_line_number = p.claim_line_number
where a.place_of_service_code = '11'

