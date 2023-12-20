{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
/*
    Denominator logic for invalid value tests is dependent on whether that
    specific field is populated or not. We created a macro to automatically
    generate the CTE. All invalid value tests must have a
    test_category = 'invalid_values' in the catalog seed.
*/
with professional_denominator as (

    select
          cast('professional' as {{ dbt.type_string() }} ) as test_denominator_name
        , cast(count(distinct claim_id||data_source) as int) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('normalized_input__medical_claim') }}
    where claim_type = 'professional'

)

, institutional_denominator as (

    select
          cast('institutional' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id||data_source) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('normalized_input__medical_claim') }}
    where claim_type = 'institutional'

)

, all_claim_denominator as (

    select
          cast('all' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct claim_id||data_source) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('normalized_input__medical_claim') }}
    where claim_type is not null

)

, invalid_value_denominators as (

    {{ medical_claim_denominator_invalid_values(builtins.ref('normalized_input__medical_claim')) }}

)

select * from institutional_denominator
union all 
select * from professional_denominator
union all
select * from all_claim_denominator
union all
select * from invalid_value_denominators