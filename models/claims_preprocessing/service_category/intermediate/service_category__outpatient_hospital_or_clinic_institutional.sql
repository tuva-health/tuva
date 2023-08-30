{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select distinct 
  a.claim_id
, 'Outpatient Hospital or Clinic' as service_category_2
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
left join {{ ref('service_category__emergency_department_institutional') }} b
  on a.claim_id = b.claim_id
left join {{ ref('service_category__urgent_care_institutional') }} c
  on a.claim_id = c.claim_id
where a.claim_type = 'institutional'
  and left(a.bill_type_code,2) in ('13','71','73')
  and b.claim_id is null
  and c.claim_id is null
  