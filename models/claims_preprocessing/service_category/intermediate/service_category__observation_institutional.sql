{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct 
    med.claim_id
    , 'Emergency Department' as service_category_2
    , 'Emergency Department' as service_category_3
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} med
where claim_type = 'institutional'
and (revenue_center_code in ('0760','0761','0762','0769')
or hcpcs_code in ('G0378','G0379',)
)