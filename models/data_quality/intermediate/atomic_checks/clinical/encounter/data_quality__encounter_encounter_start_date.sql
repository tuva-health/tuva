{{ config(
    enabled = var('clinical_enabled', False)
) }}

SELECT
    M.Data_SOURCE
    ,coalesce(M.ENCOUNTER_START_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'ENCOUNTER' AS TABLE_NAME
    ,'Encounter ID' as DRILL_DOWN_KEY
    ,IFNULL(ENCOUNTER_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'ENCOUNTER_START_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.ENCOUNTER_START_DATE > '{{ var('tuva_last_run') }}' THEN 'invalid'
        WHEN M.ENCOUNTER_START_DATE <= cast('1901-01-01' as date) THEN 'invalid'
        WHEN M.ENCOUNTER_START_DATE > M.ENCOUNTER_END_DATE THEN 'invalid'
        WHEN M.ENCOUNTER_START_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.ENCOUNTER_START_DATE > '{{ var('tuva_last_run') }}' THEN 'future'
        WHEN M.ENCOUNTER_START_DATE <= cast('1901-01-01' as date) THEN 'too old'
        WHEN M.ENCOUNTER_START_DATE > M.ENCOUNTER_END_DATE THEN 'Encounter start date after encounter end date'
        else null
    END AS INVALID_REASON
    ,CAST(ENCOUNTER_START_DATE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('encounter')}} M