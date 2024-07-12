{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

WITH cte AS
(
    SELECT DISTINCT location_id, npi, name
    FROM {{ ref('core__location')}}
)

SELECT 
    e.encounter_ID,
    CASE WHEN S.Encounter_ID IS NULL THEN 'Not Classified' ELSE S.ED_CLASSIFICATION_DESCRIPTION END AS ED_CLASSIFICATION_DESCRIPTION,
    CASE 
        WHEN S.Encounter_ID IS NULL THEN 'Non-Avoidable'
        WHEN CAST(S.ed_classification_order AS INT) <= 3 THEN s.ED_CLASSIFICATION_DESCRIPTION
        ELSE 'Non-Avoidable' END AS avoidable_category,
    e.PAID_AMOUNT,
    e.primary_diagnosis_code,
    e.primary_diagnosis_description,
    e.primary_diagnosis_code || ' | ' || e.primary_diagnosis_description AS Primary_Diagnosis_and_Description,
    P.CCSR_PARENT_CATEGORY,
    P.CCSR_Category,
    P.CCSR_CATEGORY_DESCRIPTION,
    P.CCSR_Category || ' | '|| P.CCSR_CATEGORY_DESCRIPTION AS CCSR_CATEGORY_AND_DESCRIPTION,
    B.BODY_SYSTEM,
    e.facility_id,
    e.allowed_amount,
    e.charge_amount,
    e.data_source,
    e.length_of_stay,
    e.discharge_disposition_code || ' | ' || e.discharge_disposition_description AS discharge_code_and_Description,
    e.patient_id || '|' || e.data_source AS patient_source_key,
    e.facility_name,
    e.encounter_start_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('core__encounter')}} e
LEFT JOIN {{ ref('ed_classification__summary') }} S ON e.encounter_id = s.encounter_id
LEFT JOIN cte ON e.facility_id = cte.location_id
LEFT JOIN {{ ref('ccsr__dx_vertical_pivot') }} AS P
  ON e.primary_diagnosis_code = P.code
  AND P.ccsr_category_rank = 1
LEFT JOIN {{ ref('ccsr__dxccsr_v2023_1_body_systems') }} B ON P.CCSR_PARENT_CATEGORY = B.CCSR_PARENT_CATEGORY
WHERE e.encounter_type = 'emergency department'
