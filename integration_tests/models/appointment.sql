{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_columns -%}
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
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , normalized_status as x_temp_normalized_status #}
    {# , appointment_specialty as x_temp_appointment_specialty #}
    {# , start_datetime as zzz_temp_start_datetime #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_name
    , ingest_datetime
{%- endset -%}

{% if var('use_synthetic_data') == true -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ ref('appointment_seed') }}

{%- else -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ source('source_input', 'appointment') }}

{%- endif %}
