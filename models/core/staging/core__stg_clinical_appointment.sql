{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

select
      cast(appointment_id as {{ dbt.type_string() }}) as appointment_id
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(source_appointment_type_code as {{ dbt.type_string() }}) as source_appointment_type_code
    , cast(source_appointment_type_description as {{ dbt.type_string() }}) as source_appointment_type_description
    , cast(normalized_appointment_type_code as {{ dbt.type_string() }}) as normalized_appointment_type_code
    , cast(normalized_appointment_type_description as {{ dbt.type_string() }}) as normalized_appointment_type_description
    , {{ try_to_cast_datetime('start_datetime') }} as start_datetime
    , {{ try_to_cast_datetime('end_datetime') }} as end_datetime
    , cast(duration as {{ dbt.type_int() }}) as duration
    , cast(location_id as {{ dbt.type_string() }}) as location_id
    , cast(practitioner_id as {{ dbt.type_string() }}) as practitioner_id
    , cast(source_status as {{ dbt.type_string() }}) as source_status
    , cast(normalized_status as {{ dbt.type_string() }}) as normalized_status
    , cast(appointment_specialty as {{ dbt.type_string() }}) as appointment_specialty
    , cast(reason as {{ dbt.type_string() }}) as reason
    , cast(source_reason_code_type as {{ dbt.type_string() }}) as source_reason_code_type
    , cast(source_reason_code as {{ dbt.type_string() }}) as source_reason_code
    , cast(source_reason_description as {{ dbt.type_string() }}) as source_reason_description
    , cast(normalized_reason_code_type as {{ dbt.type_string() }}) as normalized_reason_code_type
    , cast(normalized_reason_code as {{ dbt.type_string() }}) as normalized_reason_code
    , cast(normalized_reason_description as {{ dbt.type_string() }}) as normalized_reason_description
    , cast(cancellation_reason as {{ dbt.type_string() }}) as cancellation_reason
    , cast(source_cancellation_reason_code_type as {{ dbt.type_string() }}) as source_cancellation_reason_code_type
    , cast(source_cancellation_reason_code as {{ dbt.type_string() }}) as source_cancellation_reason_code
    , cast(source_cancellation_reason_description as {{ dbt.type_string() }}) as source_cancellation_reason_description
    , cast(normalized_cancellation_reason_code_type as {{ dbt.type_string() }}) as normalized_cancellation_reason_code_type
    , cast(normalized_cancellation_reason_code as {{ dbt.type_string() }}) as normalized_cancellation_reason_code
    , cast(normalized_cancellation_reason_description as {{ dbt.type_string() }}) as normalized_cancellation_reason_description
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('input_layer__appointment') }}
