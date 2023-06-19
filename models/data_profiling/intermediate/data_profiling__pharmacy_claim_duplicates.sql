{{ config(
     enabled = var('data_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct
    'pharmacy_claim' as source_table
  , 'all' as claim_type
  , 'claim_id' as grain
  ,  claim_id   
  , 'duplicate_values' as test_category
  , 'duplicate pharmacy claims' as test_name
  , '{{ var('last_update')}}' as last_update
from {{ ref('pharmacy_claim') }} 
group by
    claim_id
    , claim_line_number
having count(*) > 1