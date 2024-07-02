{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

WITH cte AS (
    SELECT DISTINCT location_id, npi, name
    FROM {{ ref('core__location')}}
)

SELECT e.*,
    CONCAT(e.patient_id, '|', e.data_source) AS patient_source_key,
    CONCAT(e.encounter_id, '|', e.data_source) AS encounter_source_key,
    CONCAT(e.ms_drg_code, ' | ', e.ms_drg_description) AS DRGwithDescription,
    CONCAT(e.primary_diagnosis_code, ' | ', e.primary_diagnosis_description) AS Primary_Diagnosis_and_Description,
    CONCAT(e.admit_source_code, ' | ', e.admit_source_description) AS Admit_Source_code_and_Description,
    CONCAT(e.admit_type_code, ' | ', e.admit_type_description) AS admit_type_code_and_description,
    CONCAT(e.discharge_disposition_code, ' | ', e.discharge_disposition_description) AS discharge_code_and_description,
    P.CCSR_PARENT_CATEGORY,
    P.CCSR_Category,
    P.CCSR_CATEGORY_DESCRIPTION,
    CONCAT(P.CCSR_Category, ' | ', P.CCSR_CATEGORY_DESCRIPTION) AS CCSR_CATEGORY_AND_DESCRIPTION,
    B.BODY_SYSTEM
FROM {{ ref('core__encounter')}} e
LEFT JOIN cte l ON e.facility_id = l.location_id
LEFT JOIN {{ ref('ccsr__dx_vertical_pivot') }} P ON e.primary_diagnosis_code = P.Code AND P.CCSR_CATEGORY_RANK = 1
LEFT JOIN {{ ref('ccsr__dxccsr_v2023_1_body_systems') }} B ON P.CCSR_PARENT_CATEGORY = B.CCSR_PARENT_CATEGORY
WHERE e.encounter_type = 'acute inpatient'
