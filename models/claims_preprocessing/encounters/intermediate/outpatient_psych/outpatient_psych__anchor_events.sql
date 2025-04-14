
with service_category as (
  select distinct
      claim_id
    , patient_data_source_id
    , start_date
  from {{ ref('encounters__stg_medical_claim') }}
  where
    service_category_2 = 'outpatient psychiatric' --both inst and prof as anchor
)

select distinct
claim_id
, '{{ var('tuva_last_run') }}' as tuva_last_run
from service_category
