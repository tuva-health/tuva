{{ config(
     enabled = var('hedis_enabled', False) == True | as_bool
   )
}}
select
      measure
    , measure_year
    , patient
    , cql_key
    , cql_value
    , data_source
    , file_name
    , file_date
from {{ ref('hedis_cql_engine_log') }}