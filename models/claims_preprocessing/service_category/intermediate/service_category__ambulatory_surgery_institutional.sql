{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with multiple_sources as (
select distinct
    med.claim_id
    , 'ambulatory surgery center' as service_category_2
    , 'ambulatory surgery center' as service_category_3
    , '{{ this.name }}' as source_model_name
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_outpatient_institutional') }} as outpatient
    on med.claim_id = outpatient.claim_id
where revenue_center_code in ('0490', '0499')

union all

select distinct
    med.claim_id
    , 'ambulatory surgery center' as service_category_2
    , 'ambulatory surgery center' as service_category_3
    , '{{ this.name }}' as source_model_name
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_outpatient_institutional') }} as outpatient
    on med.claim_id = outpatient.claim_id
where med.primary_taxonomy_code = '261QA1903X'
)

select distinct claim_id
, 'outpatient' as service_category_1
, service_category_2
, service_category_3
, source_model_name
, tuva_last_run
from multiple_sources
