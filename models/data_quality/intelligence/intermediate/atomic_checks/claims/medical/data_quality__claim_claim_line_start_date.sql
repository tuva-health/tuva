{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT  
    M.Data_SOURCE
    ,coalesce(M.CLAIM_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,IFNULL(M.CLAIM_ID, 'NULL') || '|' || IFNULL(TO_VARCHAR(M.CLAIM_LINE_NUMBER), 'NULL')  AS DRILL_DOWN_VALUE
    ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'CLAIM_LINE_START_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.CLAIM_LINE_START_DATE > CURRENT_DATE() THEN 'invalid'
        WHEN M.CLAIM_LINE_START_DATE < DATEADD(year, -10, CURRENT_DATE()) THEN 'invalid'
        WHEN M.CLAIM_LINE_START_DATE < M.CLAIM_START_DATE THEN 'invalid'
        WHEN M.CLAIM_END_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.CLAIM_LINE_START_DATE > CURRENT_DATE() THEN 'future'
        WHEN M.CLAIM_LINE_START_DATE < DATEADD(year, -10, CURRENT_DATE()) THEN 'too old'
        WHEN M.CLAIM_LINE_START_DATE < M.CLAIM_START_DATE THEN 'line date less than than claim date'
        else null
    END AS INVALID_REASON
    ,CAST(CLAIM_LINE_START_DATE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ source('tuva_claim_input','medical_claim') }} M