{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}


  select distinct  
  med.claim_id
, med.claim_line_number
, med.claim_line_id
, 'office-based' as service_category_1    
, 'office-based other' as service_category_2
, 'office-based other' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
  from {{ ref('service_category__stg_office_based') }} med
  left join {{ ref('service_category__pharmacy_professional') }} pharm on med.claim_line_id = pharm.claim_line_id
  left join {{ ref('service_category__office_based_radiology') }} rad on med.claim_line_id = rad.claim_line_id
  left join {{ ref('service_category__office_based_visit_professional') }} visit on med.claim_line_id = visit.claim_line_id
  left join {{ ref('service_category__office_based_surgery_professional') }} surg on med.claim_line_id = surg.claim_line_id
  left join {{ ref('service_category__office_based_physical_therapy_professional') }} pt on med.claim_line_id = pt.claim_line_id
  where pharm.claim_line_id is null 
  and rad.claim_line_id is null 
  and visit.claim_line_id is null
  and surg.claim_line_id is null
  and pt.claim_line_id is null


