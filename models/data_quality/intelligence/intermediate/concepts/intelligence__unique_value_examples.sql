{{ config(
     enabled = var('clinical_enabled',False)
   )
}}

SELECT * FROM {{ ref('intelligence__primary_keys_condition_condition_id') }}

UNION

SELECT * FROM {{ ref('intelligence__primary_keys_encounter_encounter_id') }}

UNION

SELECT * FROM {{ ref('intelligence__primary_keys_lab_result_lab_result_id') }}

UNION

SELECT * FROM {{ ref('intelligence__primary_keys_location_location_id') }}

UNION

SELECT * FROM {{ ref('intelligence__primary_keys_medication_medication_id') }}

UNION

SELECT * FROM {{ ref('intelligence__primary_keys_observation_observation_id') }}

UNION

SELECT * FROM {{ ref('intelligence__primary_keys_patient_patient_id') }}

UNION

SELECT * FROM {{ ref('intelligence__primary_keys_practitioner_practitioner_id') }}

UNION

SELECT * FROM {{ ref('intelligence__primary_keys_procedure_procedure_id') }}