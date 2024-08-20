{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct 
  med.claim_id
, 'Outpatient Hospice' as service_category_2
, 'Outpatient Hospice' as service_category_3
,'{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} med
inner join {{ ref('service_category__stg_outpatient_institutional') }} outpatient
    on med.claim_id = outpatient.claim_id
where substring(bill_type_code, 1, 2) in ('81')

OR 
( med.hcpcs_code in ('Q5001','Q5002','Q5003','Q5009') 
AND substring(med.bill_type_code, 1, 2) NOT IN ('31','32','33')
)

OR
med.revenue_center_code in ('0651','0652')

  