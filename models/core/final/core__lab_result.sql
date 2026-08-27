{{ config(
     enabled = the_tuva_project.tuva_boolean_var('clinical_enabled', false)
   )
}}

select
      labs.lab_result_id
    , labs.person_id
    , labs.patient_id
    , labs.encounter_id
    , labs.accession_number
    , labs.source_order_type
    , labs.source_order_code
    , labs.source_order_description
    , labs.source_component_type
    , labs.source_component_code
    , labs.source_component_description
    , labs.normalized_order_type
    , labs.normalized_order_code
    , labs.normalized_order_description
    , labs.normalized_component_type
    , labs.normalized_component_code
    , labs.normalized_component_description
    , labs.status
    , labs.result
    , labs.result_datetime
    , labs.collection_datetime
    , labs.source_units
    , labs.normalized_units
    , labs.source_reference_range_low
    , labs.source_reference_range_high
    , labs.normalized_reference_range_low
    , labs.normalized_reference_range_high
    , labs.source_abnormal_code
    , labs.normalized_abnormal_code
    , labs.specimen
    , labs.ordering_practitioner_id
    {{ select_extension_columns(ref('normalized__lab_result'), alias='labs') }}
    , labs.ingest_datetime
    , labs.tuva_last_run
    , labs.data_source
from {{ ref('normalized__lab_result') }} as labs
