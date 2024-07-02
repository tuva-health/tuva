{{ config(
    enabled = var('clinical_enabled', False)
) }}

SELECT
    M.Data_SOURCE
    ,coalesce(M.RECORDED_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'CONDITION' AS TABLE_NAME
    ,'Condition ID' as DRILL_DOWN_KEY
    , coalesce(condition_id, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'RESOLVED_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.RESOLVED_DATE > '{{ var('tuva_last_run') }}' THEN 'invalid'
        WHEN M.RESOLVED_DATE <= cast('1901-01-01' as date) THEN 'invalid'
        WHEN M.RESOLVED_DATE < M.ONSET_DATE THEN 'invalid'
        WHEN M.RESOLVED_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.RESOLVED_DATE > '{{ var('tuva_last_run') }}' THEN 'future'
        WHEN M.RESOLVED_DATE <= cast('1901-01-01' as date) THEN 'too old'
        WHEN M.RESOLVED_DATE < M.ONSET_DATE THEN 'Resolved date before onset date'
        else null
    END AS INVALID_REASON
    ,CAST(RESOLVED_DATE AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('condition')}} M