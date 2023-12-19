{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with test_detail_union as(

select * from {{ ref('data_quality__claims_preprocessing_test_detail_stage_medical_claim') }}
union all
select * from {{ ref('data_quality__claims_preprocessing_test_detail_stage_eligibility') }}
union all
select * from {{ ref('data_quality__claims_preprocessing_test_detail_stage_pharmacy_claim') }}


)

select 
    source_table
    , case 
        when source_table = 'normalized_input__medical_claim' and test_category = 'duplicate_claims'
            then '1_duplicate_claims'
        when source_table = 'normalized_input__medical_claim' and test_category = 'claim_type'
            then '2_claim_type'
        when source_table = 'normalized_input__medical_claim' and test_category = 'header'
            then '3_header'
        when source_table = 'normalized_input__medical_claim' and test_category = 'invalid_values'
            then '4_invalid_values'
        when source_table = 'normalized_input__medical_claim' and test_category = 'missing_values'
            then '5_missing_values'
        when source_table = 'normalized_input__medical_claim' and test_category = 'plausibility'
            then '6_plausibility'            
        when source_table = 'normalized_input__eligibility' and test_category = 'duplicate_eligibility'
            then '1_duplicate_eligibility'
        when source_table = 'normalized_input__eligibility' and test_category = 'invalid_values'
            then '2_invalid_values'
        when source_table = 'normalized_input__eligibility' and test_category = 'missing_values'
            then '3_missing_values'
        when source_table = 'normalized_input__eligibility' and test_category = 'plausibility'
            then '4_plausibility'
        when source_table = 'normalized_input__pharmacy_claim' and test_category = 'duplicate_claims'
            then '1_duplicate_claims'
        when source_table = 'normalized_input__pharmacy_claim' and test_category = 'missing_values'
            then '2_missing_values'
        when source_table = 'normalized_input__pharmacy_claim' and test_category = 'plausibility'
            then '3_plausibility'
        else test_category 
      end as test_category
    , test_name 
    , grain
    , claim_type
    , pipeline_test
    , foreign_key
    , data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from test_detail_union