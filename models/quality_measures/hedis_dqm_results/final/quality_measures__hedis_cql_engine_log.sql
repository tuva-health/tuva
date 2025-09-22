{{ config(
     enabled = var('hedis_enabled', False) == True | as_bool
   )
}}

with intermediate as (

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
    from {{ ref('quality_measures__int_hedis_cql_engine_log') }}

)

select
      patient as person_id
    , {{ string_extract('measure', start_delim='(', end_delim=')') }} as measure_id
    , measure as measure_name
    , measure_year as measure_version
    , cql_key as cql_concept_key
    , cql_value as cql_concept_value
    , data_source
    , file_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from intermediate
where row_num = 1
