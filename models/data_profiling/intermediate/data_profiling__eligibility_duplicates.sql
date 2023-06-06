{{ config(
     enabled = var('data_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct
    'eligibility' as source_table
  , 'all' as claim_type
  , 'patient_id' as grain
  ,  patient_id   
  , 'duplicate_values' as test_category
  , 'duplicate eligibility' as test_name
from {{ ref('eligibility') }} 
group by
    patient_id
    , member_id
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
having count(*) > 1