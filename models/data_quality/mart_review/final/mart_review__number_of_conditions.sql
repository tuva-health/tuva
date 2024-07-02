{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

WITH xwalk AS (
    SELECT DISTINCT patient_id, data_source
    FROM {{ ref('core__patient')}}
),
cte AS (
    SELECT L.PATIENT_ID,
           x.data_source,
           COUNT(*) AS NumofConditions
    FROM {{ ref('chronic_conditions__tuva_chronic_conditions_long') }} L
    LEFT JOIN xwalk x ON L.patient_id = x.patient_id
    GROUP BY L.PATIENT_ID, x.data_source
)
SELECT P.Patient_ID,
       P.data_source,
       CONCAT(P.Patient_ID, '|', P.data_source) AS Patient_Source_key,
       COALESCE(CTE.NumofConditions, 0) AS NumofConditions
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('core__patient')}} P
LEFT JOIN cte ON P.PATIENT_ID = CTE.PATIENT_ID AND P.data_source = CTE.data_source
