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
        , 1 as criteria
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
        , 2 as criteria
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
        , 3 as criteria
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

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(age as integer) as age
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(denominator_flag as integer) as denominator_flag
    from final_denominator

)

select 
      patient_id
    , age
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
