select
      measure
    , measure_year
    , patient
    , key
    , value
from {{ ref('hedis_cql_engine_log_seed') }}