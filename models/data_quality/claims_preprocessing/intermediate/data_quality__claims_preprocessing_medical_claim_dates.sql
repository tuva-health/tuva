{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set claim_date_column_list = [
      'claim_start_date'
    , 'claim_end_date'
    , 'claim_line_start_date'
    , 'claim_line_end_date'
    , 'paid_date'
    , 'procedure_date_1'
    , 'procedure_date_2'
    , 'procedure_date_3'
    , 'procedure_date_4'
    , 'procedure_date_5'
    , 'procedure_date_6'
    , 'procedure_date_7'
    , 'procedure_date_8'
    , 'procedure_date_9'
    , 'procedure_date_10'
    , 'procedure_date_11'
    , 'procedure_date_12'
    , 'procedure_date_13'
    , 'procedure_date_14'
    , 'procedure_date_15'
    , 'procedure_date_16'
    , 'procedure_date_17'
    , 'procedure_date_18'
    , 'procedure_date_19'
    , 'procedure_date_20'
    , 'procedure_date_21'
    , 'procedure_date_22'
    , 'procedure_date_23'
    , 'procedure_date_24'
    , 'procedure_date_25'
] -%}

with claim_dates as (

 {{ medical_claim_date_check(builtins.ref('normalized_input__medical_claim'), claim_date_column_list) }}

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
    , 'claim_id' as grain
    , claim_dates.claim_id
    , claim_dates.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from claim_dates
     left join test_catalog
       on test_catalog.test_name = claim_dates.column_checked||' invalid'
       and test_catalog.source_table = 'normalized_input__medical_claim'
group by
      claim_dates.claim_id
    , claim_dates.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test