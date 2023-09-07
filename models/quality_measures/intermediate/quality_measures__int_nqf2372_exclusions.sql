{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

/*
    Eligible population from the denominator model before exclusions
*/
with denominator as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from {{ ref('quality_measures__int_nqf2372_denominator') }}

)

, advanced_illness as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_nqf2372_exclude_advanced_illness') }}

)

, dementia as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_nqf2372_exclude_dementia') }}

)

, hospice as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_nqf2372_exclude_hospice') }}

)

, institutional as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_nqf2372_exclude_institutional') }}

)

, mastectomy as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_nqf2372_exclude_mastectomy') }}

)

, palliative as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_nqf2372_exclude_palliative') }}

)

, denominator_with_advanced_illness as (

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , case
            when advanced_illness.patient_id is not null then 1
            else 0
          end as exclusion_flag
        , advanced_illness.exclusion_date
        , advanced_illness.exclusion_reason
    from denominator
         left join advanced_illness
            on denominator.patient_id = advanced_illness.patient_id

)

, denominator_with_dementia as (

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , case
            when dementia.patient_id is not null then 1
            else 0
          end as exclusion_flag
        , dementia.exclusion_date
        , dementia.exclusion_reason
    from denominator
         left join dementia
            on denominator.patient_id = dementia.patient_id

)

, denominator_with_hospice as (

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , case
            when hospice.patient_id is not null then 1
            else 0
          end as exclusion_flag
        , hospice.exclusion_date
        , hospice.exclusion_reason
    from denominator
         left join hospice
            on denominator.patient_id = hospice.patient_id

)

, denominator_with_institutional as (

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , case
            when institutional.patient_id is not null then 1
            else 0
          end as exclusion_flag
        , institutional.exclusion_date
        , institutional.exclusion_reason
    from denominator
         left join institutional
            on denominator.patient_id = institutional.patient_id

)

, denominator_with_mastectomy as (

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , case
            when mastectomy.patient_id is not null then 1
            else 0
          end as exclusion_flag
        , mastectomy.exclusion_date
        , mastectomy.exclusion_reason
    from denominator
         left join mastectomy
            on denominator.patient_id = mastectomy.patient_id

)

, denominator_with_palliative as (

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , case
            when palliative.patient_id is not null then 1
            else 0
          end as exclusion_flag
        , palliative.exclusion_date
        , palliative.exclusion_reason
    from denominator
         left join palliative
            on denominator.patient_id = palliative.patient_id

)

, exclusions_unioned as (

    select * from denominator_with_advanced_illness
    union all
    select * from denominator_with_dementia
    union all
    select * from denominator_with_hospice
    union all
    select * from denominator_with_institutional
    union all
    select * from denominator_with_mastectomy
    union all
    select * from denominator_with_palliative

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , cast(exclusion_flag as integer) as exclusion_flag
    from exclusions_unioned

)

select
      patient_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types