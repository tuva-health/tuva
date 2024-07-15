{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.RESULT_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'LAB_RESULT' AS TABLE_NAME
    ,'Lab Result ID' as DRILL_DOWN_KEY
    , coalesce(lab_result_id, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'RESULT_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.RESULT_DATE > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'invalid'
        WHEN M.RESULT_DATE <= cast('1901-01-01' as date) THEN 'invalid'
        WHEN M.RESULT_DATE < M.COLLECTION_DATE THEN 'invalid'
        WHEN M.RESULT_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.RESULT_DATE > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'future'
        WHEN M.RESULT_DATE <= cast('1901-01-01' as date) THEN 'too old'
        WHEN M.RESULT_DATE < M.COLLECTION_DATE THEN 'Result date before collection date'
        else null
    END AS INVALID_REASON
    ,CAST(RESULT_DATE as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('lab_result')}} M
            