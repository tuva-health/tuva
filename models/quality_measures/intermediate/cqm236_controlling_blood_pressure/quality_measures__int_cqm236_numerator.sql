{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with denominator as (

    select * from {{ref('quality_measures__int_cqm236_denominator')}}

)

, encounters as (

    select * from {{ref('quality_measures__stg_core__encounter')}}

)

, observations as (

    select * from {{ref('quality_measures__stg_core__observation')}}
    where lower(normalized_description) in 
        (
              'systolic blood pressure'
            , 'diastolic blood pressure'
        )

)

, observations_within_range as (

    select
        observations.*
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , denominator.performance_period_begin
        , denominator.performance_period_end
    from observations
    inner join denominator
        on observations.patient_id = denominator.patient_id
        and observations.observation_date between 
            denominator.performance_period_begin and denominator.performance_period_end

)

, observations_with_encounters as (

    select
        observations_within_range.*
        , case
            when lower(encounters.encounter_type) in (
                 'emergency department'
                ,'acute inpatient'
            )
            then 1
            else 0
          end as is_excluded
    from observations_within_range
    left join encounters
        on observations_within_range.patient_id = encounters.patient_id
        and observations_within_range.observation_date between 
            encounters.encounter_start_date and encounters.encounter_end_date
)

, valid_observations as (

    select
        *
    from observations_with_encounters
    where is_excluded != 1

)

, systolic_bp_observations as (

    select
          patient_id
        , observation_date
        , result
        , measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
        , row_number() over(partition by patient_id, observation_date order by observation_date desc, result asc) as rn
    from valid_observations
    where lower(normalized_description) = 'systolic blood pressure'

)

, diastolic_bp_observations as (

    select
          patient_id
        , observation_date
        , result
        , row_number() over(partition by patient_id, observation_date order by observation_date desc, result asc) as rn
    from valid_observations
    where lower(normalized_description) = 'diastolic blood pressure'

)

, least_systolic_bp_per_day as (

    select
          patient_id
        , observation_date
        , result as systolic_bp
        , measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
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
          least_systolic_bp_per_day.*
        , least_diastolic_bp_per_day.diastolic_bp
    from least_systolic_bp_per_day
    inner join least_diastolic_bp_per_day
        on least_systolic_bp_per_day.patient_id = least_diastolic_bp_per_day.patient_id
            and least_systolic_bp_per_day.observation_date = least_diastolic_bp_per_day.observation_date

)

, numerator as (

    select
          *
        , case
            when systolic_bp < 140 and diastolic_bp < 110 --needs to be changed back to 90; 110 for testing against synthetic data
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
