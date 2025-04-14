
with service_category as (
  select distinct
      claim_id
    , patient_data_source_id
    , start_date
  from {{ ref('encounters__stg_medical_claim') }}
  where
    service_category_2 = 'ambulance' --both inst and prof

)

select distinct
claim_id
, '{{ var('tuva_last_run') }}' as tuva_last_run
from service_category
