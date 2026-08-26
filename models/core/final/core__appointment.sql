{{ config(
     enabled = var('clinical_enabled', False)
 | as_bool
   )
}}

select
      appts.appointment_id
    , appts.person_id
    , appts.patient_id
    , appts.encounter_id
    , appts.start_datetime
    , appts.end_datetime
    , appts.duration
    , appts.location_id
    , appts.practitioner_id
    , appts.type_code
    , appts.type_description
    , appts.type_code_norm
    , appts.type_description_norm
    , appts.status_code
    , appts.status_description
    , appts.status_code_norm
    , appts.status_description_norm
    , appts.reason
    , appts.cancellation_reason
    , appts.cancellation_reason_code_norm
    , appts.cancellation_reason_description_norm
    {{ select_extension_columns(ref('normalized__appointment'), alias='appts', strip_prefix=false) }}
    , appts.ingest_datetime
    , appts.tuva_last_run
    , appts.data_source
from {{ ref('normalized__appointment') }} as appts
