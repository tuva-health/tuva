{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.DISPENSING_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'MEDICATION' AS TABLE_NAME
    ,'Medication ID' as DRILL_DOWN_KEY
    ,, coalesce(medication_id, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'DISPENSING_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.DISPENSING_DATE > '{{ var('tuva_last_run') }}' THEN 'invalid'
        WHEN M.DISPENSING_DATE <= cast('1901-01-01' as date) THEN 'invalid'
        WHEN M.DISPENSING_DATE < M.PRESCRIBING_DATE THEN 'invalid'
        WHEN M.DISPENSING_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.DISPENSING_DATE > '{{ var('tuva_last_run') }}' THEN 'future'
        WHEN M.DISPENSING_DATE <= cast('1901-01-01' as date) THEN 'too old'
        WHEN M.DISPENSING_DATE < M.PRESCRIBING_DATE THEN 'Dispensing date before prescribing date'
        else null
    END AS INVALID_REASON
    ,CAST(DISPENSING_DATE AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('medication')}} M
            