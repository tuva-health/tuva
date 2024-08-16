{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}
with multiple_sources as (

select distinct 
  m.claim_id
, 'Outpatient Psychiatric' as service_category_2
, 'Outpatient Psychiatric' as service_category_3
,'{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} m
inner join {{ ref('service_category__stg_outpatient_institutional') }} i on m.claim_id = i.claim_id
where substring(m.bill_type_code, 1, 2) in ('52')

union

select distinct 
  m.claim_id
, 'Outpatient Psychiatric' as service_category_2
, 'Outpatient Psychiatric' as service_category_3
,'{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} m
inner join {{ ref('service_category__stg_outpatient_institutional') }} i on m.claim_id = i.claim_id
where m.primary_taxonomy_code in ('283Q00000X'
                                  ,'273R00000X')
)

select distinct claim_id
,service_category_2
,service_category_3
,source_model_name
,tuva_last_run
from multiple_sources

  