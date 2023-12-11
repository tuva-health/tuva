{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    source_table
    , grain
    , test_category
    , test_name
    , claim_type
    , pipeline_test
    , count(distinct foreign_key) as failures
    , denom.denominator
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__claims_preprocessing_test_detail') }} det
inner join {{ ref('data_quality__claims_preprocessing_eligibility_denominators') }} denom
    on det.claim_type = denom.test_denominator_name
where source_table = 'eligibility'
and test_name not like '%invalid'
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
    , count(distinct foreign_key) as failures
    , denom.denominator
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__claims_preprocessing_test_detail') }} det
inner join {{ ref('data_quality__claims_preprocessing_eligibility_denominators') }} denom
    on det.test_name = denom.test_denominator_name
where source_table = 'eligibility'
and test_name like '%invalid'
group by
    source_table
    , grain
    , test_category
    , test_name
    , claim_type
    , pipeline_test
    , denom.denominator