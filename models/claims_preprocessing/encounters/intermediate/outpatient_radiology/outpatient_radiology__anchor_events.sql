
with service_category as (
  select distinct
    patient_data_source_id
    , start_date
    , hcpcs_code
  from {{ ref('encounters__stg_medical_claim') }}
  where
    service_category_2 = 'outpatient radiology' --both professional and inst
)

select distinct
    patient_data_source_id
    , start_date
    , hcpcs_code
, '{{ var('tuva_last_run') }}' as tuva_last_run
from service_category
