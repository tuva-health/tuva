{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select distinct 
    med.claim_id
    , 'Emergency Department' as service_category_2
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} med
left join {{ ref('service_category__acute_inpatient_institutional') }} inpatient
    on med.claim_id = inpatient.claim_id
where claim_type = 'institutional'
and revenue_center_code in ('0450','0451','0452','0459','0981')
and inpatient.claim_id is null
-- 0456, urgent care, is included in most published definitions
-- that also include a requirement of a bill type code for
-- inpatient or outpatient hospital.