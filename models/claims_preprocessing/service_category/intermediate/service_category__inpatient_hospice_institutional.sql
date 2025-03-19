{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct 
s.claim_id
, 'inpatient' as service_category_1
, 'inpatient hospice' as service_category_2
, 'inpatient hospice' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} s
inner join {{ ref('service_category__stg_inpatient_institutional') }} a on s.claim_id = a.claim_id
where substring(s.bill_type_code, 1, 2) in ('82')
or s.revenue_center_code in ('0655', '0656', '0658', '0115', '0125', '0135', '0145', '0155', '0235')
