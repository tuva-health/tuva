{{ config(
     enabled = var('clinical_enabled',False)
   )
}}

with cte as (
select * from {{ ref('data_quality__condition_claim_id') }}

union all

select * from {{ ref('data_quality__condition_condition_id') }}

union all

select * from {{ ref('data_quality__condition_condition_rank') }}

union all

select * from {{ ref('data_quality__condition_condition_type') }}

union all

select * from {{ ref('data_quality__condition_data_source') }}

union all

select * from {{ ref('data_quality__condition_encounter_id') }}

union all

select * from {{ ref('data_quality__condition_normalized_code_type') }}

union all

select * from {{ ref('data_quality__condition_normalized_code') }}

union all

select * from {{ ref('data_quality__condition_normalized_description') }}

union all

select * from {{ ref('data_quality__condition_onset_date') }}

union all

select * from {{ ref('data_quality__condition_patient_id') }}

union all

select * from {{ ref('data_quality__condition_present_on_admit_code') }}

union all

select * from {{ ref('data_quality__condition_present_on_admit_description') }}

union all

select * from {{ ref('data_quality__condition_recorded_date') }}

union all

select * from {{ ref('data_quality__condition_resolved_date') }}

union all

select * from {{ ref('data_quality__condition_source_code') }}

union all

select * from {{ ref('data_quality__condition_source_code_type') }}

union all

select * from {{ ref('data_quality__condition_source_description') }}

union all

select * from {{ ref('data_quality__condition_status') }}

union all

select * from {{ ref('data_quality__encounter_admit_source_code') }}

union all

select * from {{ ref('data_quality__encounter_admit_source_description') }}

union all

select * from {{ ref('data_quality__encounter_admit_type_code') }}

union all

select * from {{ ref('data_quality__encounter_admit_type_description') }}

union all

select * from {{ ref('data_quality__encounter_allowed_amount') }}

union all

select * from {{ ref('data_quality__encounter_attending_provider_id') }}

union all

select * from {{ ref('data_quality__encounter_charge_amount') }}

union all

select * from {{ ref('data_quality__encounter_data_source') }}

union all

select * from {{ ref('data_quality__encounter_discharge_disposition_code') }}

union all

select * from {{ ref('data_quality__encounter_discharge_disposition_description') }}

union all

select * from {{ ref('data_quality__encounter_encounter_end_date') }}

union all

select * from {{ ref('data_quality__encounter_encounter_id') }}

union all

select * from {{ ref('data_quality__encounter_encounter_start_date') }}

union all

select * from {{ ref('data_quality__encounter_encounter_type') }}

union all

select * from {{ ref('data_quality__encounter_facility_id') }}

union all

select * from {{ ref('data_quality__encounter_length_of_stay') }}

union all

select * from {{ ref('data_quality__encounter_drg_code') }}

union all

select * from {{ ref('data_quality__encounter_drg_description') }}

union all

select * from {{ ref('data_quality__encounter_paid_amount') }}

union all

select * from {{ ref('data_quality__encounter_patient_id') }}

union all

select * from {{ ref('data_quality__encounter_primary_diagnosis_code') }}

union all

select * from {{ ref('data_quality__encounter_primary_diagnosis_code_type') }}

union all

select * from {{ ref('data_quality__encounter_primary_diagnosis_description') }}

union all

select * from {{ ref('data_quality__practitioner_practice_affiliation') }}

union all

select * from {{ ref('data_quality__practitioner_sub_specialty') }}

union all

select * from {{ ref('data_quality__practitioner_last_name') }}

union all

select * from {{ ref('data_quality__practitioner_practitioner_id') }}

union all

select * from {{ ref('data_quality__practitioner_data_source') }}

union all

select * from {{ ref('data_quality__practitioner_npi') }}

union all

select * from {{ ref('data_quality__practitioner_first_name') }}

union all

select * from {{ ref('data_quality__practitioner_specialty') }}

union all

select * from {{ ref('data_quality__location_parent_organization') }}

union all

select * from {{ ref('data_quality__location_latitude') }}

union all

select * from {{ ref('data_quality__location_facility_type') }}

union all

select * from {{ ref('data_quality__location_zip_code') }}

union all

select * from {{ ref('data_quality__location_data_source') }}

union all

select * from {{ ref('data_quality__location_city') }}

union all

select * from {{ ref('data_quality__location_npi') }}

union all

select * from {{ ref('data_quality__location_location_id') }}

union all

select * from {{ ref('data_quality__location_longitude') }}

union all

select * from {{ ref('data_quality__location_address') }}

union all

select * from {{ ref('data_quality__location_state') }}

union all

select * from {{ ref('data_quality__location_name') }}

union all

select * from {{ ref('data_quality__procedure_normalized_code_type') }}

union all

select * from {{ ref('data_quality__procedure_normalized_description') }}

union all

select * from {{ ref('data_quality__procedure_procedure_id') }}

union all

select * from {{ ref('data_quality__procedure_claim_id') }}

union all

select * from {{ ref('data_quality__procedure_source_code') }}

union all

select * from {{ ref('data_quality__procedure_source_code_type') }}

union all

select * from {{ ref('data_quality__procedure_source_description') }}

union all

select * from {{ ref('data_quality__procedure_practitioner_id') }}

union all

select * from {{ ref('data_quality__procedure_data_source') }}

union all

select * from {{ ref('data_quality__procedure_patient_id') }}

union all

select * from {{ ref('data_quality__procedure_procedure_date') }}

union all

select * from {{ ref('data_quality__procedure_encounter_id') }}

union all

select * from {{ ref('data_quality__procedure_modifier_5') }}

union all

select * from {{ ref('data_quality__procedure_modifier_4') }}

union all

select * from {{ ref('data_quality__procedure_normalized_code') }}

union all

select * from {{ ref('data_quality__procedure_modifier_1') }}

union all

select * from {{ ref('data_quality__procedure_modifier_3') }}

union all

select * from {{ ref('data_quality__procedure_modifier_2') }}

union all

select * from {{ ref('data_quality__lab_result_source_abnormal_flag') }}

union all

select * from {{ ref('data_quality__lab_result_specimen') }}

union all

select * from {{ ref('data_quality__lab_result_source_reference_range_low') }}

union all

select * from {{ ref('data_quality__lab_result_source_units') }}

union all

select * from {{ ref('data_quality__lab_result_lab_result_id') }}

union all

select * from {{ ref('data_quality__lab_result_collection_date') }}

union all

select * from {{ ref('data_quality__lab_result_normalized_component') }}

union all

select * from {{ ref('data_quality__lab_result_ordering_practitioner_id') }}

union all

select * from {{ ref('data_quality__lab_result_result') }}

union all

select * from {{ ref('data_quality__lab_result_source_code_type') }}

union all

select * from {{ ref('data_quality__lab_result_normalized_description') }}

union all

select * from {{ ref('data_quality__lab_result_normalized_reference_range_low') }}

union all

select * from {{ ref('data_quality__lab_result_normalized_reference_range_high') }}

union all

select * from {{ ref('data_quality__lab_result_normalized_code') }}

union all

select * from {{ ref('data_quality__lab_result_source_description') }}

union all

select * from {{ ref('data_quality__lab_result_status') }}

union all

select * from {{ ref('data_quality__lab_result_accession_number') }}

union all

select * from {{ ref('data_quality__lab_result_result_date') }}

union all

select * from {{ ref('data_quality__lab_result_normalized_abnormal_flag') }}

union all

select * from {{ ref('data_quality__lab_result_data_source') }}

union all

select * from {{ ref('data_quality__lab_result_normalized_units') }}

union all

select * from {{ ref('data_quality__lab_result_normalized_code_type') }}

union all

select * from {{ ref('data_quality__lab_result_source_reference_range_high') }}

union all

select * from {{ ref('data_quality__lab_result_source_code') }}

union all

select * from {{ ref('data_quality__lab_result_patient_id') }}

union all

select * from {{ ref('data_quality__lab_result_source_component') }}

union all

select * from {{ ref('data_quality__lab_result_encounter_id') }}

union all

select * from {{ ref('data_quality__patient_sex') }}

union all

select * from {{ ref('data_quality__patient_state') }}

union all

select * from {{ ref('data_quality__patient_city') }}

union all

select * from {{ ref('data_quality__patient_longitude') }}

union all

select * from {{ ref('data_quality__patient_county') }}

union all

select * from {{ ref('data_quality__patient_race') }}

union all

select * from {{ ref('data_quality__patient_death_flag') }}

union all

select * from {{ ref('data_quality__patient_address') }}

union all

select * from {{ ref('data_quality__patient_data_source') }}

union all

select * from {{ ref('data_quality__patient_zip_code') }}

union all

select * from {{ ref('data_quality__patient_first_name') }}

union all

select * from {{ ref('data_quality__patient_last_name') }}

union all

select * from {{ ref('data_quality__patient_latitude') }}

union all

select * from {{ ref('data_quality__patient_birth_date') }}

union all

select * from {{ ref('data_quality__patient_death_date') }}

union all

select * from {{ ref('data_quality__patient_patient_id') }}

union all

select * from {{ ref('data_quality__medication_rxnorm_code') }}

union all

select * from {{ ref('data_quality__medication_source_code') }}

union all

select * from {{ ref('data_quality__medication_atc_code') }}

union all

select * from {{ ref('data_quality__medication_dispensing_date') }}

union all

select * from {{ ref('data_quality__medication_prescribing_date') }}

union all

select * from {{ ref('data_quality__medication_days_supply') }}

union all

select * from {{ ref('data_quality__medication_strength') }}

union all

select * from {{ ref('data_quality__medication_patient_id') }}

union all

select * from {{ ref('data_quality__medication_rxnorm_description') }}

union all

select * from {{ ref('data_quality__medication_encounter_id') }}

union all

select * from {{ ref('data_quality__medication_data_source') }}

union all

select * from {{ ref('data_quality__medication_atc_description') }}

union all

select * from {{ ref('data_quality__medication_quantity_unit') }}

union all

select * from {{ ref('data_quality__medication_source_description') }}

union all

select * from {{ ref('data_quality__medication_ndc_code') }}

union all

select * from {{ ref('data_quality__medication_medication_id') }}

union all

select * from {{ ref('data_quality__medication_source_code_type') }}

union all

select * from {{ ref('data_quality__medication_ndc_description') }}

union all

select * from {{ ref('data_quality__medication_quantity') }}

union all

select * from {{ ref('data_quality__medication_practitioner_id') }}

union all

select * from {{ ref('data_quality__medication_route') }}

union all

select * from {{ ref('data_quality__observation_source_code') }}

union all

select * from {{ ref('data_quality__observation_normalized_reference_range_high') }}

union all

select * from {{ ref('data_quality__observation_source_units') }}

union all

select * from {{ ref('data_quality__observation_observation_type') }}

union all

select * from {{ ref('data_quality__observation_normalized_code') }}

union all

select * from {{ ref('data_quality__observation_normalized_description') }}

union all

select * from {{ ref('data_quality__observation_data_source') }}

union all

select * from {{ ref('data_quality__observation_panel_id') }}

union all

select * from {{ ref('data_quality__observation_observation_id') }}

union all

select * from {{ ref('data_quality__observation_source_reference_range_low') }}

union all

select * from {{ ref('data_quality__observation_result') }}

union all

select * from {{ ref('data_quality__observation_source_code_type') }}

union all

select * from {{ ref('data_quality__observation_normalized_reference_range_low') }}

union all

select * from {{ ref('data_quality__observation_observation_date') }}

union all

select * from {{ ref('data_quality__observation_encounter_id') }}

union all

select * from {{ ref('data_quality__observation_source_description') }}

union all

select * from {{ ref('data_quality__observation_source_reference_range_high') }}

union all

select * from {{ ref('data_quality__observation_normalized_units') }}

union all

select * from {{ ref('data_quality__observation_normalized_code_type') }}

union all

select * from {{ ref('data_quality__observation_patient_id') }}

)

select
    data_source
    , cast(source_date as {{ dbt.type_string() }}) as source_date
    , table_name
    , drill_down_key
    , drill_down_value
    , field_name
    , bucket_name
    , invalid_reason
    , field_value
    , tuva_last_run
    , dense_rank() over (
order by data_source, table_name, field_name) + 100000 as summary_sk
from cte
