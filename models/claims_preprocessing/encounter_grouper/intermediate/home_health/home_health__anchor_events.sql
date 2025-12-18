{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with service_category as (
  select distinct
      claim_id
    , patient_data_source_id
    , start_date
  from {{ ref('encounter_grouper__stg_medical_claim') }}
  where
    service_category_2 = 'home health' -- both prof and inst as anchors

)

select distinct
claim_id
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from service_category
