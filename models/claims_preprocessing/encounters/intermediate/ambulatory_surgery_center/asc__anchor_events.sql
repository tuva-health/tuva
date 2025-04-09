{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with service_category as (
  select distinct
      claim_id
    , patient_data_source_id
    , start_date
    , end_date
  from {{ ref('encounters__stg_medical_claim') }}
  where
    service_category_2 = 'ambulatory surgery center' -- include both professional and institutional claims as anchor events

)

select distinct
claim_id
, '{{ var('tuva_last_run') }}' as tuva_last_run
from service_category
