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
FROM {{ ref('core__patient')}} P
LEFT JOIN cte ON P.PATIENT_ID = CTE.PATIENT_ID AND P.data_source = CTE.data_source
