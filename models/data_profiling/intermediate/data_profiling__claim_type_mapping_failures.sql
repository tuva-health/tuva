{{ config(
     enabled = var('data_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

with claim_type_mapping as(
    select 
        claim_id
        , claim_line_number
        , claim_type as source_claim_type
        , case
            when bill_type_code is not null or revenue_center_code is not null 
                then 'institutional'
            when place_of_service_code is not null
                then 'professional'
            else null
        end as data_profiling_claim_type
    from {{ ref('medical_claim') }} 
    )

select
    'medical_claim' as source_table
  , 'all' as claim_type
  , 'claim_id' as grain
  ,  claim_id    
  , 'claim_type' as test_category
  , 'claim_type mapping incorrect' as test_name
  , '{{ var('last_update')}}' as last_update
from claim_type_mapping
where source_claim_type <> data_profiling_claim_type
group by
    claim_id