{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}



select distinct 
  a.claim_id
, 'Acute Inpatient - Other' as service_category_2
, 'Acute Inpatient - Other' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} s
inner join {{ ref('service_category__stg_inpatient_institutional') }} a on s.claim_id = a.claim_id
left join {{ ref('service_category__acute_inpatient_institutional_maternity') }} mat on mat.claim_id = a.claim_id
left join {{ ref('service_category__acute_inpatient_institutional_med_surg') }} med on med.claim_id = a.claim_id
left join {{ ref('service_category__acute_inpatient_institutional_substance_use') }} sub on sub.claim_id = a.claim_id
left join {{ ref('service_category__inpatient_skilled_nursing_institutional') }} snf on snf.claim_id = a.claim_id
left join {{ ref('service_category__inpatient_hospice_institutional') }} hosp on hosp.claim_id = a.claim_id