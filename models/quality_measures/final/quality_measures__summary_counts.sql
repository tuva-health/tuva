{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
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
        , performance_flag
    from {{ ref('quality_measures__summary_long') }}
    where measure_id is not null

)

, calculate_performance_rate  as (

    select
          measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
        , sum(denominator_flag) as denominator_sum
        , sum(numerator_flag) as numerator_sum
        , sum(exclusion_flag) as exclusion_sum
        , (
            cast(sum(performance_flag) as {{ dbt.type_numeric() }}) /
                (cast(count(performance_flag) as {{ dbt.type_numeric() }}) )
          )*100 as performance_rate
    from summary_long
    group by
          measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end

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
        , cast(exclusion_sum as integer) as exclusion_sum
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
    , exclusion_sum
    , performance_rate
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types