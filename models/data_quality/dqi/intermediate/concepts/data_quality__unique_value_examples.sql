{{ config(
     enabled = var('clinical_enabled',False)
   )
}}

select * from {{ ref('data_quality__primary_keys_condition_condition_id') }}

union all

select * from {{ ref('data_quality__primary_keys_encounter_encounter_id') }}

union all

select * from {{ ref('data_quality__primary_keys_lab_result_lab_result_id') }}

union all

select * from {{ ref('data_quality__primary_keys_location_location_id') }}

union all

select * from {{ ref('data_quality__primary_keys_medication_medication_id') }}

union all

select * from {{ ref('data_quality__primary_keys_observation_observation_id') }}

union all

select * from {{ ref('data_quality__primary_keys_patient_patient_id') }}

union all

select * from {{ ref('data_quality__primary_keys_practitioner_practitioner_id') }}

union all

select * from {{ ref('data_quality__primary_keys_procedure_procedure_id') }}
