{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select distinct
    source_table
    , claim_type
    , grain
    , claim_id
    , test_category
    , test_name 
from {{ ref('data_profiling__pharmacy_claim_duplicates') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id
    , test_category
    , test_name 
from {{ ref('data_profiling__pharmacy_claim_missing_values') }}