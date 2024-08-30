{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct 
    med.claim_id
    ,med.claim_line_number
    ,med.claim_line_id
    ,'Outpatient' as service_category_1
    , 'Observation' as service_category_2
    , 'Observation' as service_category_3
    ,'{{ this.name }}' as source_model_name
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} med
inner join {{ ref('service_category__stg_professional') }} prof on med.claim_id = prof.claim_id 
and
med.claim_line_number = prof.claim_line_number
where hcpcs_code in ('G0378','G0379')
