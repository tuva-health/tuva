SELECT *,
       FLOOR({{ datediff('birth_date', 'tuva_last_run', 'day') }} / 365) AS PATIENT_AGE,
       CONCAT(patient_id, '|', data_source) AS patient_data_source_key
FROM {{ ref('core__patient')}}
