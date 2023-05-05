{{ config(
     enabled = var('data_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

with all_denominator as(
    select
        cast('all' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct patient_id) as denominator
    from {{ ref('input_layer__eligibility') }}
)
, gender_denominator as(
    select
        cast('gender invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct patient_id) as denominator
    from {{ ref('input_layer__eligibility') }}
    where gender is not null
)
, race_denominator as(
    select
        cast('race invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct patient_id) as denominator
    from {{ ref('input_layer__eligibility') }}
    where race is not null
)
, payer_type_denominator as(
    select
        cast('payer_type invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct patient_id) as denominator
    from {{ ref('input_layer__eligibility') }}
    where payer_type is not null
)
, dual_status_denominator as(
    select
        cast('dual_status_code invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct patient_id) as denominator
    from {{ ref('input_layer__eligibility') }}
    where dual_status_code is not null
)
, medicare_status_denominator as(
    select
        cast('medicare_status_code invalid' as {{ dbt.type_string() }} ) as test_denominator_name
        , count(distinct patient_id) as denominator
    from {{ ref('input_layer__eligibility') }}
    where medicare_status_code is not null
)
select * from all_denominator
union all
select * from gender_denominator
union all 
select * from race_denominator
union all 
select * from payer_type_denominator
union all 
select * from dual_status_denominator
union all 
select * from medicare_status_denominator
