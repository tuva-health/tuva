{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct 
    med.claim_id
    , 'Emergency Department' as service_category_2
    , 'Emergency Department' as service_category_3
    , '{{ var('tuva_last_run')}}' as tuva_last_run
    , '{{ this.name }}' as source_model_name
from {{ ref('service_category__stg_medical_claim') }} med
inner join {{ ref('service_category__stg_outpatient_institutional') }} outpatient
    on med.claim_id = outpatient.claim_id
where revenue_center_code in ('0450','0451','0452','0459','0981')

-- 0456, urgent care, is included in most published definitions
-- that also include a requirement of a bill type code for
-- inpatient or outpatient hospital.