{{ config(
     enabled = var('clinical_enabled', false)
 | as_bool
   )
}}

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
    , coalesce(type_description, type_code) as type
    , coalesce(status_description, status_code) as status
    , reason
    , cancellation_reason
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__appointment') }}
