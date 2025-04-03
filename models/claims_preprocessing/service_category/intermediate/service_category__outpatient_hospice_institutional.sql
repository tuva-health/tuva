{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select distinct
    med.claim_id
  , 'outpatient' as service_category_1
  , 'outpatient hospice' as service_category_2
  , 'outpatient hospice' as service_category_3
  , '{{ this.name }}' as source_model_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_outpatient_institutional') }} as outpatient
  on med.claim_id = outpatient.claim_id
where 
  substring(med.bill_type_code, 1, 2) in ('81')
  or (
    med.hcpcs_code in ('Q5001', 'Q5002', 'Q5003', 'Q5009')
    and NOT EXISTS (SELECT 1 FROM {{ ref('service_category__home_health_institutional') }} AS hhi WHERE med.claim_id = hhi.claim_id)
  )
  or med.revenue_center_code in ('0650', '0651', '0652', '0657', '0659')
