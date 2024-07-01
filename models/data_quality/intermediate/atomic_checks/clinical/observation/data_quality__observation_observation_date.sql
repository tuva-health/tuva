{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.OBSERVATION_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'OBSERVATION' AS TABLE_NAME
    ,'Observation ID' as DRILL_DOWN_KEY
    ,IFNULL(OBSERVATION_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'OBSERVATION_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.OBSERVATION_DATE > '{{ var('tuva_last_run') }}' THEN 'invalid'
        WHEN M.OBSERVATION_DATE <= cast('1901-01-01' as date) THEN 'invalid'
        WHEN M.OBSERVATION_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.OBSERVATION_DATE > '{{ var('tuva_last_run') }}' THEN 'future'
        WHEN M.OBSERVATION_DATE <= cast('1901-01-01' as date) THEN 'too old'
        else null
    END AS INVALID_REASON
    ,CAST(OBSERVATION_DATE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('observation')}} M