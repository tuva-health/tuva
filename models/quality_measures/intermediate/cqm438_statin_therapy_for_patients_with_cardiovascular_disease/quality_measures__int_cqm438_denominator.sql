{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with denominator_criteria_1 as (

    select 
          patient_id
        , age
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , denominator_flag
        , tuva_last_run
        , '1' as criteria
    from {{ ref('quality_measures__int_cqm438_denominator_criteria1') }}

)

, denominator_criteria_2 as (

    select 
          patient_id
        , age
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , denominator_flag
        , tuva_last_run
        , '2' as criteria
    from {{ ref('quality_measures__int_cqm438_denominator_criteria2') }}

)

, denominator_criteria_3 as (

    select 
          patient_id
        , age
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , denominator_flag
        , tuva_last_run
        , '3' as criteria
    from {{ ref('quality_measures__int_cqm438_denominator_criteria3') }}

)

, final_denominator as (

    select 
        *
    from denominator_criteria_1

    union all

    select
        *
    from denominator_criteria_2

    union all

    select
        *
    from denominator_criteria_3

)

select 
    *
from final_denominator