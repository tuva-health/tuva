{{ config(
     enabled = var('quality_measures_reporting_enabled',var('claims_enabled',var('tuva_marts_enabled',True)))
   )
}}

with summary_long as (

    select
          measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
        , denominator_flag
        , numerator_flag
        , exclusion_flag
    from {{ ref('quality_measures_reporting__summary_long') }}
    where measure_id is not null

)

, apply_exclusions as (

    select
          measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
        , case
            when exclusion_flag = 1 then null
            else denominator_flag
          end as denominator_flag_adjusted
        , case
            when exclusion_flag = 1 then null
            else numerator_flag
          end as numerator_flag_adjusted
    from summary_long

)

, sum_flags as (

    select
          measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
        , sum(denominator_flag_adjusted) as denominator_sum
        , sum(numerator_flag_adjusted) as numerator_sum
    from apply_exclusions
    group by
          measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end

)

, calculate_performance_rate as (

    select
          measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
        , denominator_sum
        , numerator_sum
        , (
            cast(numerator_sum as {{ dbt.type_numeric() }}) /
                cast(denominator_sum as {{ dbt.type_numeric() }})
          )*100 as performance_rate
    from sum_flags

)

, add_data_types as (

    select
          cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(denominator_sum as integer) as denominator_sum
        , cast(numerator_sum as integer) as numerator_sum
        , round(cast(performance_rate as {{ dbt.type_numeric() }}),3) as performance_rate
    from calculate_performance_rate

)

select
      measure_id
    , measure_name
    , measure_version
    , performance_period_begin
    , performance_period_end
    , denominator_sum
    , numerator_sum
    , performance_rate
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types