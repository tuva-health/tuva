{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct 
  claim_id
, 'Outpatient Psychiatric' as service_category_2
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'institutional'
  and left(bill_type_code,2) in ('52')
  