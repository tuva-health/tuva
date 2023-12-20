{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select distinct
    source_table
    , claim_type
    , grain
    , claim_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from {{ ref('data_quality__claims_preprocessing_pharmacy_claim_duplicates') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from {{ ref('data_quality__claims_preprocessing_pharmacy_claim_missing_values') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from {{ ref('data_quality__claims_preprocessing_pharmacy_header_fail_details') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from {{ ref('data_quality__claims_preprocessing_pharmacy_claim_line_numbers') }}