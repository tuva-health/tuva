{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with denominator_ranked as (

    select
          person_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , denominator_flag
        , row_number() over (
            partition by
                  person_id
                , performance_period_begin
                , performance_period_end
                , measure_id
                , measure_name
            order by
                case when performance_period_end is null then 1 else 0 end
                , performance_period_end desc
          ) as rn
    from {{ ref('quality_measures__int_adhras_denominator') }}

)

, denominator as (

    select
          person_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , denominator_flag
    from denominator_ranked
    where rn = 1

)

, numerator_ranked as (

    select
          person_id
        , evidence_date
        , evidence_value
        , row_number() over (
            partition by person_id
            order by
                case when evidence_date is null then 1 else 0 end
                , evidence_date desc
          ) as rn
    from {{ ref('quality_measures__int_adhras_numerator') }}

)

, numerator as (

    select
          person_id
        , evidence_date
        , evidence_value
    from numerator_ranked
    where rn = 1

)

, exclusions_ranked as (

    select
          person_id
        , exclusion_date
        , exclusion_reason
        , row_number() over (
            partition by person_id
            order by
                case when exclusion_date is null then 1 else 0 end
                , exclusion_date desc
          ) as rn
    from {{ ref('quality_measures__int_adhras_exclusions') }}

)

, exclusions as (

    select
          person_id
        , exclusion_date
        , exclusion_reason
    from exclusions_ranked
    where rn = 1

)

, measure_flags as (

    select
          denominator.person_id
        , 1 as denominator_flag
        , case
            when numerator.person_id is not null then 1
            else 0
          end as numerator_flag
        , case
            when exclusions.person_id is not null then 1
            else 0
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
    from denominator
        left outer join numerator
            on denominator.person_id = numerator.person_id
        left outer join exclusions
            on denominator.person_id = exclusions.person_id

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
    from measure_flags

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
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from add_data_types
