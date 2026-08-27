{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

with service_category as (
  select distinct
      claim_id
    , data_source
    , patient_data_source_id
    , start_date
  from {{ ref('encounters__stg_medical_claim') }}
  where
    service_category_2 = 'durable medical equipment' --both inst and prof

)

select distinct
claim_id
, data_source
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from service_category
