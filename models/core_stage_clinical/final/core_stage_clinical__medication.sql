select
    medication_id
    , patient_id
    , encounter_id
    , dispensing_date
    , prescribing_date
    , source_code_type
    , source_code
    , source_description
    , ndc_code
    , ndc_description
    , rx_norm_code
    , rx_norm_description
    , atc_code
    , atc_description
    , route
    , strength
    , quantity
    , quantity_unit
    , days_supply
    , practitioner_id
    , data_source
    , tuva_last_run
from {{ ref('core_stage_clinical__medication') }}