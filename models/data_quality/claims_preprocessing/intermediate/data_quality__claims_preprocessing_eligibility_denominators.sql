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
      'dual_status_code'
    , 'gender'
    , 'medicare_status_code'
    , 'original_reason_entitlement_code'
    , 'payer_type'
    , 'race'
] -%}

with all_denominator as (

    select
        cast('all' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct patient_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }}

)

, invalid_value_denominators as (

    {{ eligibility_denominator_invalid_values(builtins.ref('eligibility'), column_list) }}

)

select * from all_denominator
union all
select * from invalid_value_denominators