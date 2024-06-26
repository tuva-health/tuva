{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT  
    M.Data_SOURCE
    ,coalesce(M.PAID_DATE,'1900-01-01') AS SOURCE_DATE
    ,'PHARMACY_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,IFNULL(M.CLAIM_ID, 'NULL') || '|' || IFNULL(TO_VARCHAR(M.CLAIM_LINE_NUMBER), 'NULL')  AS DRILL_DOWN_VALUE
    ,'PHARMACY' AS CLAIM_TYPE
    ,'DISPENSING_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.DISPENSING_DATE > CURRENT_DATE() THEN 'invalid'
        WHEN M.DISPENSING_DATE < DATEADD(year, -10, CURRENT_DATE()) THEN 'invalid'
        WHEN M.DISPENSING_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.DISPENSING_DATE > CURRENT_DATE() THEN 'future'
        WHEN M.DISPENSING_DATE < DATEADD(year, -10, CURRENT_DATE()) THEN 'too old'
        else null
        END AS INVALID_REASON
    ,CAST(DISPENSING_DATE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ source('tuva_claim_input','pharmacy_claim') }} M