{{ config(
     enabled = the_tuva_project.tuva_boolean_var('clinical_enabled', false)
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
    , 'appointment' as x_tuva_test_extension
    , 'appointment' as ext_tuva_test_extension
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__appointment') }}
