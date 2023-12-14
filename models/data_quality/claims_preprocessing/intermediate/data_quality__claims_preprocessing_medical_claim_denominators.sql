{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
/*
    Denominator logic for invalid value tests is dependent on whether that
    specific field is populated or not. For new invalid value tests, add the
    column to this list and a macro will generate the necessary CTE. These
    tests must have a test_category = 'invalid_values' in the catalog seed.
*/
{% set column_list = [
      'admit_source_code'
    , 'admit_type_code'
    , 'apr_drg_code'
    , 'bill_type_code'
    , 'billing_npi'
    , 'claim_type'
    , 'diagnosis_code_1'
    , 'diagnosis_code_type'
    , 'diagnosis_poa_1'
    , 'discharge_disposition_code'
    , 'facility_npi'
    , 'ms_drg_code'
    , 'place_of_service_code'
    , 'procedure_code_type'
    , 'rendering_npi'
    , 'revenue_center_code'
] -%}

with professional_denominator as (

    select
          cast('professional' as {{ dbt.type_string() }} ) as test_denominator_name
        , cast(count(distinct claim_id) as int) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where claim_type = 'professional'

)

, institutional_denominator as (

    select
          cast('institutional' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where claim_type = 'institutional'

)

, all_claim_denominator as (

    select
          cast('all' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }}
    where claim_type is not null

)

, invalid_value_denominators as (

    {{ medical_claim_denominator_invalid_values(builtins.ref('medical_claim'), column_list) }}

)

select * from institutional_denominator
union all 
select * from professional_denominator
union all
select * from all_claim_denominator
union all
select * from invalid_value_denominators