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
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('hedis_cql_engine_log') }}