{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct 
  claim_id
, 'Lab' as service_category_2
, '{{ var('last_update')}}' as last_update
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'institutional'
  and left(bill_type_code,2) in ('14')
  