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
from {{ ref('data_profiling__institutional_header_fail_details') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id
    , test_category
    , test_name 
from {{ ref('data_profiling__professional_header_fail_details') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id
    , test_category
    , test_name 
from {{ ref('data_profiling__medical_claim_inst_missing_values') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id
    , test_category
    , test_name 
from {{ ref('data_profiling__medical_claim_prof_missing_values') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id
    , test_category
    , test_name 
from {{ ref('data_profiling__medical_claim_invalid_values') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id
    , test_category
    , test_name 
from {{ ref('data_profiling__claim_type_unmapped') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id
    , test_category
    , test_name 
from {{ ref('data_profiling__claim_type_mapping_failures') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id
    , test_category
    , test_name 
from {{ ref('data_profiling__medical_claim_duplicates') }}
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id
    , test_category
    , test_name 
from {{ ref('data_profiling__medical_claim_plausibility') }}