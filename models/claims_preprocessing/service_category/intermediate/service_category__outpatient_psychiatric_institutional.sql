{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}
with multiple_sources as (

select distinct 
  m.claim_id
, 'outpatient psychiatric' as service_category_2
, 'outpatient psychiatric' as service_category_3
,'{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} m
inner join {{ ref('service_category__stg_outpatient_institutional') }} i on m.claim_id = i.claim_id
where m.revenue_center_code in ('0513', '0905')

union all

select distinct 
  m.claim_id
, 'outpatient psychiatric' as service_category_2
, 'outpatient psychiatric' as service_category_3
,'{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} m
inner join {{ ref('service_category__stg_outpatient_institutional') }} i on m.claim_id = i.claim_id
where m.primary_taxonomy_code in ('283Q00000X'
                                  ,'273R00000X')
)

select distinct claim_id
,'outpatient' as service_category_1    
,service_category_2
,service_category_3
,source_model_name
,tuva_last_run
from multiple_sources
