{{ config(
     enabled = var('data_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

select
    'medical_claim' as source_table
  , 'all' as claim_type
  , 'claim_id' as grain
  ,  claim_id    
  , 'claim_type' as test_category
  , 'claim_type missing' as test_name
from {{ ref('input_layer__medical_claim') }} 
where claim_type is null
group by
    claim_id