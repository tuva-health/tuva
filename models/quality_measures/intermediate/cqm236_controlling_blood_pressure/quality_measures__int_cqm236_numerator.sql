{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with denominator as (

    select * from {{ref('quality_measures__int_cqm236_denominator')}}

)

, systolic_bp_observations as (

    select
          patient_id
        , observation_date
        , result
        , row_number() over(partition by patient_id, observation_date order by observation_date desc, result asc) as rn
    from tuva_synthetic.core.observation
    where lower(normalized_description) = 'systolic blood pressure'

)

, diastolic_bp_observations as (

    select
          patient_id
        , observation_date
        , result
        , row_number() over(partition by patient_id, observation_date order by observation_date desc, result asc) as rn
    from tuva_synthetic.core.observation
    where lower(normalized_description) = 'diastolic blood pressure'

)

, least_systolic_bp_per_day as (

    select
          patient_id
        , observation_date
        , result as systolic_bp
    from systolic_bp_observations
    where rn = 1

)

, least_diastolic_bp_per_day as (

    select
          patient_id
        , observation_date
        , result as diastolic_bp
    from systolic_bp_observations
    where rn = 1

)

, patients_with_bp_readings as (

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , coalesce(least_systolic_bp_per_day.observation_date, least_diastolic_bp_per_day.observation_date) as observation_date
        , least_systolic_bp_per_day.systolic_bp
        , least_diastolic_bp_per_day.diastolic_bp
    from denominator
    left join least_systolic_bp_per_day
        on denominator.patient_id = least_systolic_bp_per_day.patient_id
    left join least_diastolic_bp_per_day
        on denominator.patient_id = least_diastolic_bp_per_day.patient_id
    where (least_systolic_bp_per_day.observation_date between denominator.performance_period_begin and denominator.performance_period_end)
        and (least_diastolic_bp_per_day.observation_date between denominator.performance_period_begin and denominator.performance_period_end)

)

, numerator as (

    select
          *
        , case
            when systolic_bp < 140 and diastolic_bp < 90
            then 1
            else 0
          end as numerator_flag
    from patients_with_bp_readings

)

, add_data_types as (

     select distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(observation_date as date) as observation_date
        , cast(numerator_flag as integer) as numerator_flag
    from numerator

)

select
      patient_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , observation_date
    , numerator_flag
from add_data_types
