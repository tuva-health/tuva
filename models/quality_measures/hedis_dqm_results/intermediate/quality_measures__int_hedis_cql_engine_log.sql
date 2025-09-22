{{ config(
     enabled = var('hedis_enabled', False) == True | as_bool
   )
}}

with stage as (

    select
          measure
        , measure_year
        , patient
        , cql_key
        , cql_value
        , data_source
        , file_name
        , file_date
    from {{ ref('quality_measures__stg_hedis_cql_engine_log') }}

)

/*
    add row number to sort rows from previous executions
    for the same measure, year, patient, and key
*/
, add_row_number as (

    select
          measure
        , measure_year
        , patient
        , cql_key
        , cql_value
        , data_source
        , file_name
        , file_date
        , row_number() over(
            partition by
                  measure
                , measure_year
                , patient
                , cql_key
            order by
                  file_date desc
                , file_name desc
        ) as row_num
    from stage

)

select
      patient
    , measure
    , measure_year
    , cql_key
    , cql_value
    , data_source
    , file_name
    , file_date
    , row_num
from add_row_number
