{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with multiple_sources as (
select distinct 
    med.claim_id
    , med.claim_line_number
    , 'Outpatient Pharmacy' as service_category_2
    , 'Outpatient Pharmacy' as service_category_3
    ,'{{ this.name }}' as source_model_name
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} med
inner join {{ ref('service_category__stg_outpatient_institutional') }} outpatient
    on med.claim_id = outpatient.claim_id
and (substring(revenue_center_code,1,3) in ('025' --pharmacy
,'026' --iv therapy
,'063' --pharmacy
,'089' --pharmacy
)
or revenue_center_code = '0547'
or ccs_category = '240' --medications
)

UNION 

select distinct 
    med.claim_id
    , med.claim_line_number
    , 'Inpatient'
    , 'Inpatient Pharmacy' as service_category_2
    , 'Inpatient Pharmacy' as service_category_3
    ,'{{ this.name }}' as source_model_name
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} med
inner join {{ ref('service_category__stg_inpatient_institutional') }} outpatient
    on med.claim_id = outpatient.claim_id
and (substring(revenue_center_code,1,3) in ('025' --pharmacy
,'026' --iv therapy
,'063' --pharmacy
,'089' --pharmacy
)
or revenue_center_code = '0547'
)
)

select claim_id
,claim_line_number
,service_category_1
,service_category_2
,service_category_3
,source_model_name
,tuva_last_run
from multiple_sources