{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct 
    med.claim_id
    , 'Ambulatory Sugery Center' as service_category_2
    , 'Ambulatory Sugery Center' as service_category_3
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} med
left join {{ ref('service_category__acute_inpatient_institutional') }} inpatient
    on med.claim_id = inpatient.claim_id
where claim_type = 'institutional'
and revenue_center_code in ('0490','0499')
and inpatient.claim_id is null
