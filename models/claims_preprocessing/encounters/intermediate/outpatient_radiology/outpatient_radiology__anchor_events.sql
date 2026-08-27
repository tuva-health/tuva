{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

with service_category as (
  select distinct
    patient_data_source_id
    , data_source
    , start_date
    , hcpcs_code
  from {{ ref('encounters__stg_medical_claim') }}
  where
    service_category_2 = 'outpatient radiology' --both professional and inst
)

select distinct
    patient_data_source_id
    , data_source
    , start_date
    , hcpcs_code
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from service_category
