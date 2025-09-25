{{ config(
     enabled = var('hedis_enabled', False) == True | as_bool
   )
}}
with stage as (

    select *
    from {{ ref('quality_measures__stg_hedis_measure_report') }}

)

, add_measure_year as (

    select
          id
        , measure
        , measure_id
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
    from stage

)

/*
    add row number to sort rows from previous executions
    for the same measure, year, and patient
*/
, add_row_number as (

    select
          id
        , measure
        , measure_id
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
        , row_number() over(
            partition by
                  measure
                , measure_year
                , patient
            order by
                  file_date desc
                , file_name desc
        ) as row_num
    from add_measure_year

)

select
      id
    , measure
    , measure_id
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
from add_row_number
