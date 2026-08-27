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
    , type_code
    , type_description
    , status_code
    , status_description
    , reason
    , cancellation_reason
    , 'appointment' as x_tuva_test_extension
    , 'appointment' as ext_tuva_test_extension
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__appointment') }}
