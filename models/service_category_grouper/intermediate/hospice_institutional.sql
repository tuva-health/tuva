{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct 
  claim_id
, 'Hospice' as service_category_2
from {{ ref('input_layer__medical_claim') }}
where claim_type = 'institutional'
  and left(bill_type_code,2) in ('81','82')
  