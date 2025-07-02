select
    cast(null as {{ dbt.type_string() }}) as immunization_id
    , cast(null as {{ dbt.type_string() }}) as person_id
    , cast(null as {{ dbt.type_string() }}) as patient_id
    , cast(null as {{ dbt.type_string() }}) as encounter_id
    , cast(null as {{ dbt.type_string() }}) as source_code_type
    , cast(null as {{ dbt.type_string() }}) as source_code
    , cast(null as {{ dbt.type_string() }}) as source_description
    , cast(null as {{ dbt.type_string() }}) as normalized_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_code
    , cast(null as {{ dbt.type_string() }}) as normalized_description
    , cast(null as {{ dbt.type_string() }}) as status
    , cast(null as {{ dbt.type_string() }}) as status_reason
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as occurrence_date
    , cast(null as {{ dbt.type_string() }}) as source_dose
    , cast(null as {{ dbt.type_string() }}) as normalized_dose
    , cast(null as {{ dbt.type_string() }}) as lot_number
    , cast(null as {{ dbt.type_string() }}) as body_site
    , cast(null as {{ dbt.type_string() }}) as route
    , cast(null as {{ dbt.type_string() }}) as location_id
    , cast(null as {{ dbt.type_string() }}) as practitioner_id
    , cast(null as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
limit 0
