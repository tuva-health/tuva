{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with denominator as (

    select
          person_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , denominator_flag
    from {{ ref('quality_measures__int_adh_statins_denominator') }}

)

, numerator as (

    select
          person_id
        , evidence_date
        , evidence_value
    from {{ ref('quality_measures__int_adh_statins_numerator') }}

)

, exclusions as (

    select
          person_id
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_adh_statins_exclusions') }}

)

, measure_flags as (

    select
          denominator.person_id
        , case
            when denominator.person_id is not null
            then 1
            else null
          end as denominator_flag
        , case
            when numerator.person_id is not null and denominator.person_id is not null
            then 1
            when denominator.person_id is not null
            then 0
            else null
          end as numerator_flag
        , case
            when exclusions.person_id is not null and denominator.person_id is not null
            then 1
            when denominator.person_id is not null
            then 0
            else null
          end as exclusion_flag
        , numerator.evidence_date
        , numerator.evidence_value
        , exclusions.exclusion_date
        , exclusions.exclusion_reason
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , (row_number() over (
            partition by
                  denominator.person_id
                , denominator.performance_period_begin
                , denominator.performance_period_end
                , denominator.measure_id
                , denominator.measure_name
              order by
                  case when numerator.evidence_date is null then 1 else 0 end
                  , numerator.evidence_date desc
                , case when exclusions.exclusion_date is null then 1 else 0 end
                  , exclusions.exclusion_date desc
          )) as rn
    from denominator
        left outer join numerator
            on denominator.person_id = numerator.person_id
        left outer join exclusions
            on denominator.person_id = exclusions.person_id

)

, deduped as (

    select
          person_id
        , denominator_flag
        , numerator_flag
        , exclusion_flag
        , evidence_date
        , evidence_value
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
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(denominator_flag as integer) as denominator_flag
        , cast(numerator_flag as integer) as numerator_flag
        , cast(exclusion_flag as integer) as exclusion_flag
        , cast(evidence_date as date) as evidence_date
        , cast(evidence_value as {{ dbt.type_string() }}) as evidence_value
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
      person_id
    , denominator_flag
    , numerator_flag
    , exclusion_flag
    , evidence_date
    , evidence_value
    , exclusion_date
    , exclusion_reason
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
