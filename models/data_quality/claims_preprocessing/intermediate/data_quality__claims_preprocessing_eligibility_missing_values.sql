{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set eligibility_missing_column_list = [
      'patient_id'
    , 'member_id'
    , 'gender'
    , 'race'
    , 'birth_date'
    , 'death_date'
    , 'death_flag'
    , 'enrollment_start_date'
    , 'enrollment_end_date'
    , 'payer'
    , 'payer_type'
    , 'dual_status_code'
    , 'medicare_status_code'
    , 'first_name'
    , 'last_name'
    , 'address'
    , 'city'
    , 'state'
    , 'zip_code'
    , 'phone'
    , 'data_source'
] -%}

with eligibility_missing as (

 {{ eligibility_missing_column_check(builtins.ref('normalized_input__eligibility'), eligibility_missing_column_list) }}

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
    from {{ ref('data_quality__test_catalog') }}

)

select
      test_catalog.source_table
    , 'all' as claim_type
    , 'patient_id' as grain
    , eligibility_missing.patient_id
    , eligibility_missing.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from eligibility_missing
     left join test_catalog
       on test_catalog.test_name = eligibility_missing.column_checked||' missing'
       and test_catalog.source_table = 'normalized_input__eligibility'
group by
      eligibility_missing.patient_id
    , eligibility_missing.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test