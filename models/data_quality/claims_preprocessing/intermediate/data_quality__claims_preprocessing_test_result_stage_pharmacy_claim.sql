{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with pharmacy_claim_denominator as(
  select 
    cast('all' as {{ dbt.type_string() }} ) as claim_type
    , cast(count(distinct claim_id||data_source) as int) as count
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_string() }} ) as tuva_last_run
  from {{ ref('normalized_input__pharmacy_claim') }}
)

, distinct_patient_per_category as(
    select
        source_table
        , grain
        , test_category
        , test_name
        , claim_type
        , pipeline_test
        , count(distinct foreign_key||data_source) as failures
    from {{ ref('data_quality__claims_preprocessing_test_detail') }}
    where source_table = 'normalized_input__pharmacy_claim'
    group by
        source_table
        , grain
        , test_category
        , test_name
        , claim_type
        , pipeline_test
    )

  select
    source_table
    , grain
    , claim.test_category
    , claim.test_name
    , claim.claim_type
    , pipeline_test
    , claim.failures
    , denom.count as denominator
    , tuva_last_run
  from distinct_patient_per_category claim
  left join pharmacy_claim_denominator denom
      on claim.claim_type = denom.claim_type
  group by
    source_table
    , grain
    , claim.test_category
    , claim.test_name
    , claim.claim_type
    , pipeline_test
    , claim.failures
    , denom.count
    , tuva_last_run