{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('use_synthetic_data') == true -%}

select
      appointment_id
    , person_id
    , patient_id
    , encounter_id
    , source_appointment_type_code
    , source_appointment_type_description
    , normalized_appointment_type_code
    , normalized_appointment_type_description
    , start_datetime
    , end_datetime
    , duration
    , location_id
    , practitioner_id
    , source_appointment_type_code as type_code
    , source_appointment_type_description as type_description
    , source_status as status_code
    , source_status as status_description
    , source_status
    , normalized_status
    , appointment_specialty
    , reason
    , source_reason_code_type
    , source_reason_code
    , source_reason_description
    , normalized_reason_code_type
    , normalized_reason_code
    , normalized_reason_description
    , cancellation_reason
    , source_cancellation_reason_code_type
    , source_cancellation_reason_code
    , source_cancellation_reason_description
    , normalized_cancellation_reason_code_type
    , normalized_cancellation_reason_code
    , normalized_cancellation_reason_description
    , data_source
from {{ ref('raw_data__appointment') }}

{%- else -%}

select
      appointment_id
    , person_id
    , patient_id
    , encounter_id
    , type_code as source_appointment_type_code
    , type_description as source_appointment_type_description
    , cast(null as {{ dbt.type_string() }}) as normalized_appointment_type_code
    , cast(null as {{ dbt.type_string() }}) as normalized_appointment_type_description
    , start_datetime
    , end_datetime
    , duration
    , location_id
    , practitioner_id
    , type_code
    , type_description
    , status_code
    , status_description
    , status_code as source_status
    , cast(null as {{ dbt.type_string() }}) as normalized_status
    , cast(null as {{ dbt.type_string() }}) as appointment_specialty
    , reason
    , cast(null as {{ dbt.type_string() }}) as source_reason_code_type
    , cast(null as {{ dbt.type_string() }}) as source_reason_code
    , cast(null as {{ dbt.type_string() }}) as source_reason_description
    , cast(null as {{ dbt.type_string() }}) as normalized_reason_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_reason_code
    , cast(null as {{ dbt.type_string() }}) as normalized_reason_description
    , cancellation_reason
    , cast(null as {{ dbt.type_string() }}) as source_cancellation_reason_code_type
    , cast(null as {{ dbt.type_string() }}) as source_cancellation_reason_code
    , cast(null as {{ dbt.type_string() }}) as source_cancellation_reason_description
    , cast(null as {{ dbt.type_string() }}) as normalized_cancellation_reason_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_cancellation_reason_code
    , cast(null as {{ dbt.type_string() }}) as normalized_cancellation_reason_description
    , data_source
from {{ source('source_input', 'appointment') }}

{%- endif %}
