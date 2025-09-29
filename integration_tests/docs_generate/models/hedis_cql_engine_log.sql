select
      measure
    , measure_id
    , measure_year
    , patient
    , cql_key
    , cql_value
    , data_source
    , file_name
    , file_date
from {{ ref('hedis_cql_engine_log_seed') }}