{{ config(
     enabled = var('data_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

with test_detail_union as(
    /******  medical claim  ******/
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
    from {{ ref('data_profiling__medical_claim_missing_values') }}
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
    /******  eligibility  ******/

    union all
    select distinct
        source_table
        , claim_type
        , grain
        , patient_id
        , test_category
        , test_name 
    from {{ ref('data_profiling__eligibility_duplicates') }}
    union all
    select distinct
        source_table
        , claim_type
        , grain
        , patient_id
        , test_category
        , test_name 
    from {{ ref('data_profiling__eligibility_missing_values') }}
    union all
    select distinct
        source_table
        , claim_type
        , grain
        , patient_id
        , test_category
        , test_name 
    from {{ ref('data_profiling__eligibility_invalid_values') }}
    union all
    select distinct
        source_table
        , claim_type
        , grain
        , patient_id
        , test_category
        , test_name 
    from {{ ref('data_profiling__eligibility_plausibility') }}
    /******  pharmacy_claim  ******/

    union all
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
)

select 
    source_table
    , case 
        when source_table = 'medical_claim' and test_category = 'duplicate_claims'
            then '1_duplicate_claims'
        when source_table = 'medical_claim' and test_category = 'claim_type'
            then '2_claim_type'
        when source_table = 'medical_claim' and test_category = 'header'
            then '3_header'
        when source_table = 'medical_claim' and test_category = 'invalid_values'
            then '4_invalid_values'
        when source_table = 'medical_claim' and test_category = 'missing_values'
            then '5_missing_values'
        when source_table = 'medical_claim' and test_category = 'plausibility'
            then '6_plausibility'            
        when source_table = 'eligibility' and test_category = 'duplicate_eligibility'
            then '1_duplicate_eligibility'
        when source_table = 'eligibility' and test_category = 'invalid_values'
            then '2_invalid_values'
        when source_table = 'eligibility' and test_category = 'missing_values'
            then '3_missing_values'
        when source_table = 'eligibility' and test_category = 'plausibility'
            then '4_plausibility'
        when source_table = 'pharmacy_claim' and test_category = 'duplicate_claims'
            then '1_duplicate_claims'
        when source_table = 'pharmacy_claim' and test_category = 'missing_values'
            then '2_missing_values'
        when source_table = 'pharmacy_claim' and test_category = 'plausibility'
            then '3_plausibility'
        else test_category 
    end as test_category
    , test_name 
    , grain
    , claim_type
    , claim_id as foreign_key
    , '{{ var('last_update')}}' as last_update
from test_detail_union