{{ config(
     enabled = var('clinical_enabled', False) | as_bool
   )
}}

select
      medication_id
    , person_id
    , patient_id
    , encounter_id
    , dispensing_date
    , prescribing_date
    , source_code_type
    , source_code
    , source_description
    , ndc_code
    , rxnorm_code
    , atc_code
    , route
    , strength
    , quantity
    , quantity_unit
    , days_supply
    , practitioner_id
    , 'medication' as x_tuva_test_extension
    , 'medication' as ext_tuva_test_extension
    , ingest_datetime
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__medication') }}
