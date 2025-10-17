select
      cast(performance_year as {{ dbt.type_int() }}) as performance_year
    , cast(aco_id as {{ dbt.type_string() }}) as aco_id
    , cast(aco_lbn as {{ dbt.type_string() }}) as aco_lbn
    , cast(assignment_methodology as {{ dbt.type_string() }}) as assignment_methodology
    , cast(file_date as date) as file_date
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
from {{source('tuva_dev','provider_attr_assignment_methodology')}}
