{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

with service_category as (
  select distinct
      claim_id
    , patient_data_source_id
    , start_date
  from {{ ref('int_encounter__claim_line') }}
  where
    service_category_2 = 'dialysis' --both inst and professional as anchor

)

select distinct
claim_id
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from service_category
