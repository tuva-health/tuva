{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct 
  claim_id
, claim_line_number
, 'Outpatient Psychiatric' as service_category_2
, '{{ var('last_update')}}' as last_update
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'professional'
  and place_of_service_code in ('52','53','57','58')
  