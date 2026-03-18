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
    , start_datetime
    , end_datetime
    , duration
    , location_id
    , practitioner_id
    , source_appointment_type_code as type_code
    , source_appointment_type_description as type_description
    , source_status as status_code
    , source_status as status_description
    , reason
    , cancellation_reason
    , data_source
from {{ ref('appointment_seed') }}

{%- else -%}

select
      appointment_id
    , person_id
    , patient_id
    , encounter_id
    , start_datetime
    , end_datetime
    , duration
    , location_id
    , practitioner_id
    , type_code
    , type_description
    , status_code
    , status_description
    , reason
    , cancellation_reason
    , data_source
from {{ source('source_input', 'appointment') }}

{%- endif %}
