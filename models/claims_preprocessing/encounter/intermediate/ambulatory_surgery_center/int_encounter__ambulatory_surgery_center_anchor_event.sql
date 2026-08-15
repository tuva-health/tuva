{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

with service_category as (
  select distinct
      claim_id
    , patient_data_source_id
    , start_date
    , end_date
  from {{ ref('int_encounter__claim_line') }}
  where
    service_category_2 = 'ambulatory surgery center' -- include both professional and institutional claims as anchor events

)

select distinct
claim_id
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from service_category
