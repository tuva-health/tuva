{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
s.claim_id
, 'inpatient' as service_category_1
, 'inpatient psychiatric' as service_category_2
, 'inpatient psychiatric' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as s
inner join {{ ref('service_category__stg_inpatient_institutional') }} as i on s.claim_id = i.claim_id
where s.primary_taxonomy_code in ('283Q00000X'
                                  , '273R00000X')
