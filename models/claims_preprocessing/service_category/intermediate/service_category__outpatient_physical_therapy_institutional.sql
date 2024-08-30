{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct 
    med.claim_id
    , med.claim_line_number
    ,'Outpatient' as service_category_1    
    , 'Outpatient PT/OT/ST' as service_category_2
    , 'Outpatient PT/OT/ST' as service_category_3
    ,'{{ this.name }}' as source_model_name
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} med
inner join {{ ref('service_category__stg_outpatient_institutional') }} o on med.claim_id = o.claim_id
where ccs_category in ('213','212','215')

