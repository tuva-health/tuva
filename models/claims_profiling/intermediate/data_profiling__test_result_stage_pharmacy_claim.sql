{{ config(
     enabled = var('data_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

with pharmacy_claim_denominator as(
  select 
    cast('all' as {{ dbt.type_string() }} ) as claim_type
    , cast(count(distinct claim_id) as int) as count
  from {{ ref('pharmacy_claim') }}
)

, distinct_patient_per_category as(
    select
        source_table
        , grain
        , test_category
        , test_name
        , claim_type
        , count(distinct foreign_key) as failures
    from {{ ref('data_profiling__test_detail') }}
    where source_table = 'pharmacy_claim'
    group by
        source_table
        , grain
        , test_category
        , test_name
        , claim_type
    )

  select
    source_table
    , grain
    , claim.test_category
    , claim.test_name
    , claim.claim_type
    , claim.failures
    , elig.count as denominator
  from distinct_patient_per_category claim
  left join pharmacy_claim_denominator elig
      on claim.claim_type = elig.claim_type
  group by
    source_table
    , grain
    , claim.test_category
    , claim.test_name
    , claim.claim_type
    , claim.failures
    , elig.count

