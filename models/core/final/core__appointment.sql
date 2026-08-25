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
    , appts.type
    , appts.status
    , appts.reason
    , appts.cancellation_reason
    {{ select_extension_columns(ref('normalized__appointment'), alias='appts', strip_prefix=false) }}
    , appts.tuva_last_run
    , appts.data_source
from {{ ref('normalized__appointment') }} as appts
