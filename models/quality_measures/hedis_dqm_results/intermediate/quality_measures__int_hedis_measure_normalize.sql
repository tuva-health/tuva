{{ config(
     enabled = var('hedis_enabled', False) == True | as_bool
   )
}}
with dedupe as (

    select
          id
        , measure
        , measure_year
        , status
        , type
        , period_start
        , period_end
        , patient
        , rate_1_id
        , rate_1_population_type_0
        , rate_1_population_count_0
        , rate_1_population_type_1
        , rate_1_population_count_1
        , rate_1_population_type_2
        , rate_1_population_count_2
        , rate_1_population_type_3
        , rate_1_population_count_3
        , rate_1_population_type_4
        , rate_1_population_count_4
        , rate_1_population_type_5
        , rate_1_population_count_5
        , rate_1_population_type_6
        , rate_1_population_count_6
        , rate_2_id
        , rate_2_population_type_0
        , rate_2_population_count_0
        , rate_2_population_type_1
        , rate_2_population_count_1
        , rate_2_population_type_2
        , rate_2_population_count_2
        , rate_2_population_type_3
        , rate_2_population_count_3
        , rate_2_population_type_4
        , rate_2_population_count_4
        , rate_2_population_type_5
        , rate_2_population_count_5
        , rate_2_population_type_6
        , rate_2_population_count_6
        , data_source
        , file_name
        , file_date
        , row_num
    from {{ ref('quality_measures__int_hedis_measure_report') }}
    where row_num = 1

)

/*
    normalize population types to expected flags:
        - denominator
        - denominator-exclusion
        - denominator-exclusion-medicare
        - denominator-medicare
        - initial-population
        - numerator
*/
, normalize_rates as (

    select
          id
        , measure
        , measure_year
        , status
        , type
        , period_start
        , period_end
        , patient
        , case
            when rate_1_population_type_0 = 'initial-population' then rate_1_population_count_0
            when rate_1_population_type_1 = 'initial-population' then rate_1_population_count_1
            when rate_1_population_type_2 = 'initial-population' then rate_1_population_count_2
            when rate_1_population_type_3 = 'initial-population' then rate_1_population_count_3
            when rate_1_population_type_4 = 'initial-population' then rate_1_population_count_4
            when rate_1_population_type_5 = 'initial-population' then rate_1_population_count_5
            when rate_1_population_type_6 = 'initial-population' then rate_1_population_count_6
            else null
          end as rate_1_initial_population_flag
        , case
            when rate_1_population_type_0 = 'denominator' then rate_1_population_count_0
            when rate_1_population_type_1 = 'denominator' then rate_1_population_count_1
            when rate_1_population_type_2 = 'denominator' then rate_1_population_count_2
            when rate_1_population_type_3 = 'denominator' then rate_1_population_count_3
            when rate_1_population_type_4 = 'denominator' then rate_1_population_count_4
            when rate_1_population_type_5 = 'denominator' then rate_1_population_count_5
            when rate_1_population_type_6 = 'denominator' then rate_1_population_count_6
            else null
          end as rate_1_denominator_flag
        , case
            when rate_1_population_type_0 = 'denominator-exclusion' then rate_1_population_count_0
            when rate_1_population_type_1 = 'denominator-exclusion' then rate_1_population_count_1
            when rate_1_population_type_2 = 'denominator-exclusion' then rate_1_population_count_2
            when rate_1_population_type_3 = 'denominator-exclusion' then rate_1_population_count_3
            when rate_1_population_type_4 = 'denominator-exclusion' then rate_1_population_count_4
            when rate_1_population_type_5 = 'denominator-exclusion' then rate_1_population_count_5
            when rate_1_population_type_6 = 'denominator-exclusion' then rate_1_population_count_6
            else null
          end as rate_1_exclusion_flag
        , case
            when rate_1_population_type_0 = 'denominator-medicare' then rate_1_population_count_0
            when rate_1_population_type_1 = 'denominator-medicare' then rate_1_population_count_1
            when rate_1_population_type_2 = 'denominator-medicare' then rate_1_population_count_2
            when rate_1_population_type_3 = 'denominator-medicare' then rate_1_population_count_3
            when rate_1_population_type_4 = 'denominator-medicare' then rate_1_population_count_4
            when rate_1_population_type_5 = 'denominator-medicare' then rate_1_population_count_5
            when rate_1_population_type_6 = 'denominator-medicare' then rate_1_population_count_6
            else null
          end as rate_1_medicare_denominator_flag
        , case
            when rate_1_population_type_0 = 'denominator-exclusion-medicare' then rate_1_population_count_0
            when rate_1_population_type_1 = 'denominator-exclusion-medicare' then rate_1_population_count_1
            when rate_1_population_type_2 = 'denominator-exclusion-medicare' then rate_1_population_count_2
            when rate_1_population_type_3 = 'denominator-exclusion-medicare' then rate_1_population_count_3
            when rate_1_population_type_4 = 'denominator-exclusion-medicare' then rate_1_population_count_4
            when rate_1_population_type_5 = 'denominator-exclusion-medicare' then rate_1_population_count_5
            when rate_1_population_type_6 = 'denominator-exclusion-medicare' then rate_1_population_count_6
            else null
          end as rate_1_medicare_exclusion_flag
        , case
            when rate_1_population_type_0 = 'numerator' then rate_1_population_count_0
            when rate_1_population_type_1 = 'numerator' then rate_1_population_count_1
            when rate_1_population_type_2 = 'numerator' then rate_1_population_count_2
            when rate_1_population_type_3 = 'numerator' then rate_1_population_count_3
            when rate_1_population_type_4 = 'numerator' then rate_1_population_count_4
            when rate_1_population_type_5 = 'numerator' then rate_1_population_count_5
            when rate_1_population_type_6 = 'numerator' then rate_1_population_count_6
            else null
          end as rate_1_numerator_flag
        , case
            when rate_2_population_type_0 = 'initial-population' then rate_2_population_count_0
            when rate_2_population_type_1 = 'initial-population' then rate_2_population_count_1
            when rate_2_population_type_2 = 'initial-population' then rate_2_population_count_2
            when rate_2_population_type_3 = 'initial-population' then rate_2_population_count_3
            when rate_2_population_type_4 = 'initial-population' then rate_2_population_count_4
            when rate_2_population_type_5 = 'initial-population' then rate_2_population_count_5
            when rate_2_population_type_6 = 'initial-population' then rate_2_population_count_6
            else null
          end as rate_2_initial_population_flag
        , case
            when rate_2_population_type_0 = 'denominator' then rate_2_population_count_0
            when rate_2_population_type_1 = 'denominator' then rate_2_population_count_1
            when rate_2_population_type_2 = 'denominator' then rate_2_population_count_2
            when rate_2_population_type_3 = 'denominator' then rate_2_population_count_3
            when rate_2_population_type_4 = 'denominator' then rate_2_population_count_4
            when rate_2_population_type_5 = 'denominator' then rate_2_population_count_5
            when rate_2_population_type_6 = 'denominator' then rate_2_population_count_6
            else null
          end as rate_2_denominator_flag
        , case
            when rate_2_population_type_0 = 'denominator-exclusion' then rate_2_population_count_0
            when rate_2_population_type_1 = 'denominator-exclusion' then rate_2_population_count_1
            when rate_2_population_type_2 = 'denominator-exclusion' then rate_2_population_count_2
            when rate_2_population_type_3 = 'denominator-exclusion' then rate_2_population_count_3
            when rate_2_population_type_4 = 'denominator-exclusion' then rate_2_population_count_4
            when rate_2_population_type_5 = 'denominator-exclusion' then rate_2_population_count_5
            when rate_2_population_type_6 = 'denominator-exclusion' then rate_2_population_count_6
            else null
          end as rate_2_exclusion_flag
        , case
            when rate_2_population_type_0 = 'denominator-medicare' then rate_2_population_count_0
            when rate_2_population_type_1 = 'denominator-medicare' then rate_2_population_count_1
            when rate_2_population_type_2 = 'denominator-medicare' then rate_2_population_count_2
            when rate_2_population_type_3 = 'denominator-medicare' then rate_2_population_count_3
            when rate_2_population_type_4 = 'denominator-medicare' then rate_2_population_count_4
            when rate_2_population_type_5 = 'denominator-medicare' then rate_2_population_count_5
            when rate_2_population_type_6 = 'denominator-medicare' then rate_2_population_count_6
            else null
          end as rate_2_medicare_denominator_flag
        , case
            when rate_2_population_type_0 = 'denominator-exclusion-medicare' then rate_2_population_count_0
            when rate_2_population_type_1 = 'denominator-exclusion-medicare' then rate_2_population_count_1
            when rate_2_population_type_2 = 'denominator-exclusion-medicare' then rate_2_population_count_2
            when rate_2_population_type_3 = 'denominator-exclusion-medicare' then rate_2_population_count_3
            when rate_2_population_type_4 = 'denominator-exclusion-medicare' then rate_2_population_count_4
            when rate_2_population_type_5 = 'denominator-exclusion-medicare' then rate_2_population_count_5
            when rate_2_population_type_6 = 'denominator-exclusion-medicare' then rate_2_population_count_6
            else null
          end as rate_2_medicare_exclusion_flag
        , case
            when rate_2_population_type_0 = 'numerator' then rate_2_population_count_0
            when rate_2_population_type_1 = 'numerator' then rate_2_population_count_1
            when rate_2_population_type_2 = 'numerator' then rate_2_population_count_2
            when rate_2_population_type_3 = 'numerator' then rate_2_population_count_3
            when rate_2_population_type_4 = 'numerator' then rate_2_population_count_4
            when rate_2_population_type_5 = 'numerator' then rate_2_population_count_5
            when rate_2_population_type_6 = 'numerator' then rate_2_population_count_6
            else null
          end as rate_2_numerator_flag
        , data_source
        , file_name
        , file_date
    from dedupe

)

/*
    Performance flags are calculated by using exclusion, numerator, and
    denominator flags. When excluded from a measure the flag is null.

    Some measures produce flags for a second rate (rate-2) in the output.
    We only create these rates when rate-2 flags are not null.
    In addition, certain measures define their own logic for exclusion
    and denominator for rate-2. When those flags are not provided,
    we default to using the exclusion and denominator from rate-1.
*/
, add_performance_rates as (

    select
          id
        , measure
        , measure_year
        , status
        , type
        , period_start
        , period_end
        , patient
        , rate_1_initial_population_flag
        , rate_1_denominator_flag
        , rate_1_exclusion_flag
        , rate_1_medicare_denominator_flag
        , rate_1_medicare_exclusion_flag
        , rate_1_numerator_flag
        , rate_2_initial_population_flag
        , rate_2_denominator_flag
        , rate_2_exclusion_flag
        , rate_2_medicare_denominator_flag
        , rate_2_medicare_exclusion_flag
        , rate_2_numerator_flag
        , case
            when rate_1_exclusion_flag = 1 then null
            when rate_1_denominator_flag = 1 and rate_1_numerator_flag = 1 then 1
            when rate_1_denominator_flag = 1 then 0
            else null
          end as rate_1_performance_flag
        , case
            when rate_1_medicare_exclusion_flag = 1 then null
            when rate_1_medicare_denominator_flag = 1 and rate_1_numerator_flag = 1 then 1
            when rate_1_medicare_denominator_flag = 1 then 0
            else null
          end as rate_1_medicare_performance_flag
        , case
            when coalesce(rate_2_denominator_flag, rate_2_exclusion_flag, rate_2_numerator_flag) is not null then
                case
                    when coalesce(rate_2_exclusion_flag, rate_1_exclusion_flag) = 1 then null
                    when coalesce(rate_2_denominator_flag, rate_1_denominator_flag) = 1 and coalesce(rate_2_numerator_flag, rate_1_numerator_flag) = 1 then 1
                    when coalesce(rate_2_denominator_flag, rate_1_denominator_flag) = 1 then 0
            else null
          end end as rate_2_performance_flag
        , case
            when coalesce(rate_2_medicare_denominator_flag, rate_2_medicare_exclusion_flag) is not null then
                case
                    when rate_2_medicare_exclusion_flag = 1 then null
                    when rate_2_medicare_denominator_flag = 1 and coalesce(rate_2_numerator_flag, rate_1_numerator_flag) = 1 then 1
                    when rate_2_medicare_denominator_flag = 1 then 0
            else null
          end end as rate_2_medicare_performance_flag
        , data_source
        , file_name
        , file_date
    from normalize_rates

)

select
      id
    , measure
    , measure_year
    , status
    , type
    , period_start
    , period_end
    , patient
    , rate_1_initial_population_flag
    , rate_1_denominator_flag
    , rate_1_exclusion_flag
    , rate_1_medicare_denominator_flag
    , rate_1_medicare_exclusion_flag
    , rate_1_numerator_flag
    , rate_2_initial_population_flag
    , rate_2_denominator_flag
    , rate_2_exclusion_flag
    , rate_2_medicare_denominator_flag
    , rate_2_medicare_exclusion_flag
    , rate_2_numerator_flag
    , rate_1_performance_flag
    , rate_1_medicare_performance_flag
    , rate_2_performance_flag
    , rate_2_medicare_performance_flag
    , data_source
    , file_name
    , file_date
from add_performance_rates
