{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select distinct
    'eligibility' as source_table
  , 'all' as claim_type
  , 'patient_id' as grain
  ,  patient_id   
  , 'duplicate_values' as test_category
  , 'duplicate eligibility' as test_name
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('eligibility') }} 
group by
    patient_id
    , member_id
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
having count(*) > 1