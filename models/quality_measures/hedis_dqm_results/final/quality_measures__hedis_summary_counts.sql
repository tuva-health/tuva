{{ config(
     enabled = var('hedis_enabled', False) == True | as_bool
   )
}}

with summary_long as (

    select
          measure_id
        , measure_name
        , measure_version
        , data_source
        , performance_period_begin
        , performance_period_end
        , rate_1_denominator_flag
        , rate_1_numerator_flag
        , rate_1_exclusion_flag
        , rate_1_medicare_denominator_flag
        , rate_1_medicare_exclusion_flag
        , rate_1_performance_flag
        , rate_1_medicare_performance_flag
        , rate_2_denominator_flag
        , rate_2_numerator_flag
        , rate_2_exclusion_flag
        , rate_2_medicare_denominator_flag
        , rate_2_medicare_exclusion_flag
        , rate_2_performance_flag
        , rate_2_medicare_performance_flag
    from {{ ref('quality_measures__hedis_summary_long') }}

)

, calculate_performance_rate_1 as (

    select
          measure_id
        , measure_name
        , measure_version
        , data_source
        , performance_period_begin
        , performance_period_end
        , sum(rate_1_denominator_flag) as rate_1_denominator_sum
        , sum(rate_1_numerator_flag) as rate_1_numerator_sum
        , sum(rate_1_exclusion_flag) as rate_1_exclusion_sum
        , (cast(sum(coalesce(rate_1_performance_flag,0)) as {{ dbt.type_numeric() }}) /
            (cast(count(coalesce(rate_1_performance_flag,0)) as {{ dbt.type_numeric() }}))
          ) * 100 as rate_1_performance_rate
    from summary_long
    group by
          measure_id
        , measure_name
        , measure_version
        , data_source
        , performance_period_begin
        , performance_period_end

)

, calculate_medicare_performance_rate_1 as (

    select
          measure_id
        , measure_name
        , measure_version
        , data_source
        , performance_period_begin
        , performance_period_end
        , sum(rate_1_medicare_denominator_flag) as rate_1_medicare_denominator_sum
        , sum(rate_1_numerator_flag) as rate_1_medicare_numerator_sum
        , sum(rate_1_medicare_exclusion_flag) as rate_1_medicare_exclusion_sum
        , (cast(sum(coalesce(rate_1_medicare_performance_flag,0)) as {{ dbt.type_numeric() }}) /
            (cast(count(coalesce(rate_1_medicare_performance_flag,0)) as {{ dbt.type_numeric() }}))
          ) * 100 as rate_1_medicare_performance_rate
    from summary_long
    group by
          measure_id
        , measure_name
        , measure_version
        , data_source
        , performance_period_begin
        , performance_period_end

)

, calculate_performance_rate_2 as (

    select
          measure_id
        , measure_name
        , measure_version
        , data_source
        , performance_period_begin
        , performance_period_end
        , sum(rate_2_denominator_flag) as rate_2_denominator_sum
        , sum(rate_2_numerator_flag) as rate_2_numerator_sum
        , sum(rate_2_exclusion_flag) as rate_2_exclusion_sum
        , (cast(sum(coalesce(rate_2_performance_flag,0)) as {{ dbt.type_numeric() }}) /
            (cast(count(coalesce(rate_2_performance_flag,0)) as {{ dbt.type_numeric() }}))
          ) * 100 as rate_2_performance_rate
    from summary_long
    group by
          measure_id
        , measure_name
        , measure_version
        , data_source
        , performance_period_begin
        , performance_period_end

)

, calculate_medicare_performance_rate_2 as (

    select
          measure_id
        , measure_name
        , measure_version
        , data_source
        , performance_period_begin
        , performance_period_end
        , sum(rate_2_medicare_denominator_flag) as rate_2_medicare_denominator_sum
        , sum(rate_2_numerator_flag) as rate_2_medicare_numerator_sum
        , sum(rate_2_medicare_exclusion_flag) as rate_2_medicare_exclusion_sum
        , (cast(sum(coalesce(rate_2_medicare_performance_flag,0)) as {{ dbt.type_numeric() }}) /
            (cast(count(coalesce(rate_2_medicare_performance_flag,0)) as {{ dbt.type_numeric() }}))
          ) * 100 as rate_2_medicare_performance_rate
    from summary_long
    group by
          measure_id
        , measure_name
        , measure_version
        , data_source
        , performance_period_begin
        , performance_period_end

)

, joined as (

    select
          calculate_performance_rate_1.measure_id
        , calculate_performance_rate_1.measure_name
        , calculate_performance_rate_1.measure_version
        , calculate_performance_rate_1.performance_period_begin
        , calculate_performance_rate_1.performance_period_end
        , calculate_performance_rate_1.rate_1_denominator_sum
        , calculate_performance_rate_1.rate_1_numerator_sum
        , calculate_performance_rate_1.rate_1_exclusion_sum
        , calculate_performance_rate_1.rate_1_performance_rate
        , calculate_medicare_performance_rate_1.rate_1_medicare_denominator_sum
        , calculate_medicare_performance_rate_1.rate_1_medicare_numerator_sum
        , calculate_medicare_performance_rate_1.rate_1_medicare_exclusion_sum
        , calculate_medicare_performance_rate_1.rate_1_medicare_performance_rate
        , calculate_performance_rate_2.rate_2_denominator_sum
        , calculate_performance_rate_2.rate_2_numerator_sum
        , calculate_performance_rate_2.rate_2_exclusion_sum
        , calculate_performance_rate_2.rate_2_performance_rate
        , calculate_medicare_performance_rate_2.rate_2_medicare_denominator_sum
        , calculate_medicare_performance_rate_2.rate_2_medicare_numerator_sum
        , calculate_medicare_performance_rate_2.rate_2_medicare_exclusion_sum
        , calculate_medicare_performance_rate_2.rate_2_medicare_performance_rate
    from calculate_performance_rate_1
        left outer join calculate_medicare_performance_rate_1
            on calculate_performance_rate_1.measure_id = calculate_medicare_performance_rate_1.measure_id
            and calculate_performance_rate_1.measure_name = calculate_medicare_performance_rate_1.measure_name
            and calculate_performance_rate_1.measure_version = calculate_medicare_performance_rate_1.measure_version
            and calculate_performance_rate_1.data_source = calculate_medicare_performance_rate_1.data_source
            and calculate_performance_rate_1.performance_period_begin = calculate_medicare_performance_rate_1.performance_period_begin
            and calculate_performance_rate_1.performance_period_end = calculate_medicare_performance_rate_1.performance_period_end
        left outer join calculate_performance_rate_2
            on calculate_performance_rate_1.measure_id = calculate_performance_rate_2.measure_id
            and calculate_performance_rate_1.measure_name = calculate_performance_rate_2.measure_name
            and calculate_performance_rate_1.measure_version = calculate_performance_rate_2.measure_version
            and calculate_performance_rate_1.data_source = calculate_performance_rate_2.data_source
            and calculate_performance_rate_1.performance_period_begin = calculate_performance_rate_2.performance_period_begin
            and calculate_performance_rate_1.performance_period_end = calculate_performance_rate_2.performance_period_end
        left outer join calculate_medicare_performance_rate_2
            on calculate_performance_rate_1.measure_id = calculate_medicare_performance_rate_2.measure_id
            and calculate_performance_rate_1.measure_name = calculate_medicare_performance_rate_2.measure_name
            and calculate_performance_rate_1.measure_version = calculate_medicare_performance_rate_2.measure_version
            and calculate_performance_rate_1.data_source = calculate_medicare_performance_rate_2.data_source
            and calculate_performance_rate_1.performance_period_begin = calculate_medicare_performance_rate_2.performance_period_begin
            and calculate_performance_rate_1.performance_period_end = calculate_medicare_performance_rate_2.performance_period_end

)

, add_data_types as (

    select
          cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(rate_1_denominator_sum as integer) as rate_1_denominator_sum
        , cast(rate_1_numerator_sum as integer) as rate_1_numerator_sum
        , cast(rate_1_exclusion_sum as integer) as rate_1_exclusion_sum
        , round(cast(rate_1_performance_rate as {{ dbt.type_numeric() }}), 3) as rate_1_performance_rate
        , cast(rate_1_medicare_denominator_sum as integer) as rate_1_medicare_denominator_sum
        , cast(rate_1_medicare_numerator_sum as integer) as rate_1_medicare_numerator_sum
        , cast(rate_1_medicare_exclusion_sum as integer) as rate_1_medicare_exclusion_sum
        , round(cast(rate_1_medicare_performance_rate as {{ dbt.type_numeric() }}), 3) as rate_1_medicare_performance_rate
        , cast(rate_2_denominator_sum as integer) as rate_2_denominator_sum
        , cast(rate_2_numerator_sum as integer) as rate_2_numerator_sum
        , cast(rate_2_exclusion_sum as integer) as rate_2_exclusion_sum
        , round(cast(rate_2_performance_rate as {{ dbt.type_numeric() }}), 3) as rate_2_performance_rate
        , cast(rate_2_medicare_denominator_sum as integer) as rate_2_medicare_denominator_sum
        , cast(rate_2_medicare_numerator_sum as integer) as rate_2_medicare_numerator_sum
        , cast(rate_2_medicare_exclusion_sum as integer) as rate_2_medicare_exclusion_sum
        , round(cast(rate_2_medicare_performance_rate as {{ dbt.type_numeric() }}), 3) as rate_2_medicare_performance_rate
    from joined

)

select
      measure_id
    , measure_name
    , measure_version
    , performance_period_begin
    , performance_period_end
    , rate_1_denominator_sum
    , rate_1_numerator_sum
    , rate_1_exclusion_sum
    , rate_1_performance_rate
    , rate_1_medicare_denominator_sum
    , rate_1_medicare_numerator_sum
    , rate_1_medicare_exclusion_sum
    , rate_1_medicare_performance_rate
    , rate_2_denominator_sum
    , rate_2_numerator_sum
    , rate_2_exclusion_sum
    , rate_2_performance_rate
    , rate_2_medicare_denominator_sum
    , rate_2_medicare_numerator_sum
    , rate_2_medicare_exclusion_sum
    , rate_2_medicare_performance_rate
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
