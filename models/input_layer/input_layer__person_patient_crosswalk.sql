select distinct
    cast(person_id as {{ dbt.type_string() }}) as person_id
    , data_source
    , member_id as source_person_id
    , 'member_id' as source_person_id_description
    , file_name
    , file_date
    , ingest_datetime
from {{ ref('input_layer__eligibility')}}