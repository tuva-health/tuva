

SELECT
    patient_data_source_id,
    old_encounter_id,
    min(start_date) as encounter_start_date,
    max(end_date) as encounter_end_date
FROM {{ ref('asc__generate_encounter_id') }}
group by patient_data_source_id
,old_encounter_id
