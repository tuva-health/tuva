{{ config(
     enabled = the_tuva_project.tuva_boolean_var('clinical_enabled', false)
   )
}}

select
      obs.observation_id
    , obs.person_id
    , obs.patient_id
    , obs.encounter_id
    , obs.panel_id
    , obs.observation_date
    , obs.observation_type
    , obs.source_code_type
    , obs.source_code
    , obs.source_description
    , obs.normalized_code_type
    , obs.normalized_code
    , obs.normalized_description
    , obs.result
    , obs.source_units
    , obs.normalized_units
    , obs.source_reference_range_low
    , obs.source_reference_range_high
    , obs.normalized_reference_range_low
    , obs.normalized_reference_range_high
    {{ select_extension_columns(ref('normalized__observation'), alias='obs') }}
    , obs.ingest_datetime
    , obs.tuva_last_run
    , obs.data_source
from {{ ref('normalized__observation') }} as obs
