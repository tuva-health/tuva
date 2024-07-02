{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.DISPENSING_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'MEDICATION' AS TABLE_NAME
    ,'Medication ID' as DRILL_DOWN_KEY
    ,IFNULL(MEDICATION_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'PRESCRIBING_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.PRESCRIBING_DATE > '{{ var('tuva_last_run') }}' THEN 'invalid'
        WHEN M.PRESCRIBING_DATE <= cast('1901-01-01' as date) THEN 'invalid'
        WHEN M.PRESCRIBING_DATE > M.DISPENSING_DATE THEN 'invalid'
        WHEN M.PRESCRIBING_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.PRESCRIBING_DATE > '{{ var('tuva_last_run') }}' THEN 'future'
        WHEN M.PRESCRIBING_DATE <= cast('1901-01-01' as date) THEN 'too old'
        WHEN M.PRESCRIBING_DATE > M.DISPENSING_DATE THEN 'Prescribing date after dispensing date'
        else null
    END AS INVALID_REASON
    ,CAST(PRESCRIBING_DATE AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('medication')}} M
            