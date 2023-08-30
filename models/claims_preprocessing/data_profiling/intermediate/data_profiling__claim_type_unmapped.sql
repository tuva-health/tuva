{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    'medical_claim' as source_table
  , 'all' as claim_type
  , 'claim_id' as grain
  ,  claim_id    
  , 'claim_type' as test_category
  , 'claim_type missing' as test_name
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim') }} 
where claim_type is null
group by
    claim_id