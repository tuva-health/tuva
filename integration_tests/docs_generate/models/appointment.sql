select
      cast(null as {{ dbt.type_string() }} ) as appointment_id
    , cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , cast(null as {{ dbt.type_string() }} ) as source_appointment_type_code
    , cast(null as {{ dbt.type_string() }} ) as source_appointment_type_description
    , cast(null as {{ dbt.type_string() }} ) as normalized_appointment_type_code
    , cast(null as {{ dbt.type_string() }} ) as normalized_appointment_type_description
    , cast(null as {{ dbt.type_timestamp() }}) as start_datetime
    , cast(null as {{ dbt.type_timestamp() }}) as end_datetime
    , cast(null as {{ dbt.type_int() }} ) as duration
    , cast(null as {{ dbt.type_string() }} ) as location_id
    , cast(null as {{ dbt.type_string() }} ) as practitioner_id
    , cast(null as {{ dbt.type_string() }} ) as source_status
    , cast(null as {{ dbt.type_string() }} ) as normalized_status
    , cast(null as {{ dbt.type_string() }} ) as appointment_specialty
    , cast(null as {{ dbt.type_string() }} ) as reason
    , cast(null as {{ dbt.type_string() }} ) as source_reason_code_type
    , cast(null as {{ dbt.type_string() }} ) as source_reason_code
    , cast(null as {{ dbt.type_string() }} ) as source_reason_description
    , cast(null as {{ dbt.type_string() }} ) as normalized_reason_code_type
    , cast(null as {{ dbt.type_string() }} ) as normalized_reason_code
    , cast(null as {{ dbt.type_string() }} ) as normalized_reason_description
    , cast(null as {{ dbt.type_string() }} ) as cancellation_reason
    , cast(null as {{ dbt.type_string() }} ) as source_cancellation_reason_code_type
    , cast(null as {{ dbt.type_string() }} ) as source_cancellation_reason_code
    , cast(null as {{ dbt.type_string() }} ) as source_cancellation_reason_description
    , cast(null as {{ dbt.type_string() }} ) as normalized_cancellation_reason_code_type
    , cast(null as {{ dbt.type_string() }} ) as normalized_cancellation_reason_code
    , cast(null as {{ dbt.type_string() }} ) as normalized_cancellation_reason_description
    , cast(null as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
limit 0