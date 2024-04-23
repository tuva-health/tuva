{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
    | as_bool
   )
}}

/* selecting the full patient population as the grain of this table */
with patient as (

    select distinct patient_id
    from {{ ref('quality_measures__stg_core__patient') }}

)

, denominator as (

    select
          *
    from {{ ref('quality_measures__int_cqm236_denominator') }}

)

, numerator as (

    select
          patient_id
        , observation_date
    from {{ ref('quality_measures__int_cqm236_numerator') }}
    where numerator_flag = 1

)

, exclusions as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_cqm236_exclusions') }}

)

, measure_flags as (

    select
          patient.patient_id
        , case
            when denominator.patient_id is not null
            then 1
            else null
          end as denominator_flag
        , case
            when numerator.patient_id is not null and denominator.patient_id is not null
            then 1
            when denominator.patient_id is not null
            then 0
            else null
          end as numerator_flag
        , case
            when exclusions.patient_id is not null and denominator.patient_id is not null
            then 1
            when denominator.patient_id is not null
            then 0
            else null
          end as exclusion_flag
        , numerator.observation_date
        , exclusions.exclusion_date
        , exclusions.exclusion_reason
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , (row_number() over(
            partition by
                  patient.patient_id
                , denominator.performance_period_begin
                , denominator.performance_period_end
                , denominator.measure_id
                , denominator.measure_name
            order by
                  numerator.observation_date desc nulls last
                , exclusions.exclusion_date desc nulls last
          )) as rn
    from patient
        left join denominator
            on patient.patient_id = denominator.patient_id
        left join numerator
            on patient.patient_id = numerator.patient_id
        left join exclusions
            on patient.patient_id = exclusions.patient_id

)

/*
    Deduplicate measure rows by latest evidence date or exclusion date
*/
, deduped as (

    select
          patient_id
        , denominator_flag
        , numerator_flag
        , exclusion_flag
        , observation_date
        , exclusion_date
        , exclusion_reason
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from measure_flags
    where rn = 1

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(denominator_flag as integer) as denominator_flag
        , cast(numerator_flag as integer) as numerator_flag
        , cast(exclusion_flag as integer) as exclusion_flag
        , cast(observation_date as date) as evidence_date
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
    from deduped

)

select
      patient_id
    , denominator_flag
    , numerator_flag
    , exclusion_flag
    , evidence_date
    , cast(null as {{ dbt.type_string() }}) as evidence_value
    , exclusion_date
    , exclusion_reason
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
