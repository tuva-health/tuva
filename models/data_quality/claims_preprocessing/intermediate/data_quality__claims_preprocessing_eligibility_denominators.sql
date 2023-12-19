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
with all_denominator as (

    select
        cast('all' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct patient_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('normalized_input__eligibility') }}

)

, invalid_value_denominators as (

    {{ eligibility_denominator_invalid_values(builtins.ref('normalized_input__eligibility')) }}

)

select * from all_denominator
union all
select * from invalid_value_denominators