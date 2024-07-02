{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.RESULT_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'LAB_RESULT' AS TABLE_NAME
    ,'Lab Result ID' as DRILL_DOWN_KEY
    ,IFNULL(LAB_RESULT_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'COLLECTION_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.COLLECTION_DATE > '{{ var('tuva_last_run') }}' THEN 'invalid'
        WHEN M.COLLECTION_DATE <= cast('1901-01-01' as date) THEN 'invalid'
        WHEN M.COLLECTION_DATE > M.RESULT_DATE THEN 'invalid'
        WHEN M.COLLECTION_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.COLLECTION_DATE > '{{ var('tuva_last_run') }}' THEN 'future'
        WHEN M.COLLECTION_DATE <= cast('1901-01-01' as date) THEN 'too old'
        WHEN M.COLLECTION_DATE > M.RESULT_DATE THEN 'Collection date after result date'
        else null
    END AS INVALID_REASON
    ,CAST(COLLECTION_DATE AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('lab_result')}} M
            