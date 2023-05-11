{{ config(
     enabled = var('data_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct
    'medical_claim' as source_table
  , 'all' as claim_type
  , 'claim_id' as grain
  ,  claim_id    
  , 'duplicate_values' as test_category
  , 'duplicate medical claims' as test_name
from {{ ref('input_layer__medical_claim') }} 
group by
    claim_id
    , claim_line_number
having count(*) > 1