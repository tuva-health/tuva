{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with service_category as (
  select distinct
      claim_id
    , patient_data_source_id
    , start_date
  from {{ ref('encounters__stg_medical_claim') }}
  where
    service_category_2 = 'dialysis' --both inst and professional as anchor

)

select distinct
claim_id
, '{{ var('tuva_last_run') }}' as tuva_last_run
from service_category
