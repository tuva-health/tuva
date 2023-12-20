{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
/*
    Tests with the category 'invalid_values' are joined to the denominator model
    on test_name since that denominator logic is dependent on whether that
    specific field is populated or not.

    All other tests are joined to the denominator model on claim_type.
*/

select
    source_table
    , grain
    , test_category
    , test_name
    , claim_type
    , pipeline_test
    , count(distinct foreign_key||data_source) as failures
    , denom.denominator
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__claims_preprocessing_test_detail') }} det
inner join {{ ref('data_quality__claims_preprocessing_medical_claim_denominators') }} denom
    on det.claim_type = denom.test_denominator_name
where source_table = 'normalized_input__medical_claim'
and test_category <> 'invalid_values'
group by
    source_table
    , grain
    , test_category
    , test_name
    , claim_type
    , pipeline_test
    , denom.denominator

union all

select
    source_table
    , grain
    , test_category
    , test_name
    , claim_type
    , pipeline_test
    , count(distinct foreign_key||data_source) as failures
    , denom.denominator
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__claims_preprocessing_test_detail') }} det
inner join {{ ref('data_quality__claims_preprocessing_medical_claim_denominators') }} denom
    on det.test_name = denom.test_denominator_name
where source_table = 'normalized_input__medical_claim'
and test_category = 'invalid_values'
group by
    source_table
    , grain
    , test_category
    , test_name
    , claim_type
    , pipeline_test
    , denom.denominator