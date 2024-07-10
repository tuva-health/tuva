{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

SELECT *,
       FLOOR({{ datediff('birth_date', 'tuva_last_run', 'day') }} / 365) AS PATIENT_AGE,
       patient_id || '|' || data_source AS patient_data_source_key,
       CASE
          WHEN FLOOR({{ datediff('birth_date', 'tuva_last_run', 'day') }} / 365) < 10 THEN '0-9'
          WHEN FLOOR({{ datediff('birth_date', 'tuva_last_run', 'day') }} / 365) < 20 THEN '10-19'
          WHEN FLOOR({{ datediff('birth_date', 'tuva_last_run', 'day') }} / 365) < 30 THEN '20-29'
          WHEN FLOOR({{ datediff('birth_date', 'tuva_last_run', 'day') }} / 365) < 40 THEN '30-39'
          WHEN FLOOR({{ datediff('birth_date', 'tuva_last_run', 'day') }} / 365) < 50 THEN '40-49'
          WHEN FLOOR({{ datediff('birth_date', 'tuva_last_run', 'day') }} / 365) < 60 THEN '50-59'
          WHEN FLOOR({{ datediff('birth_date', 'tuva_last_run', 'day') }} / 365) < 70 THEN '60-69'
          WHEN FLOOR({{ datediff('birth_date', 'tuva_last_run', 'day') }} / 365) < 80 THEN '70-79'
          WHEN FLOOR({{ datediff('birth_date', 'tuva_last_run', 'day') }} / 365) < 90 THEN '80-89'
          ELSE '90+'
       END AS age_group
FROM {{ ref('core__patient')}}
