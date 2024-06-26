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
    ,'PLAN' AS FIELD_NAME
    ,CASE 
        WHEN M.PLAN is null then 'null' else 'valid' END AS BUCKET_NAME
    ,NULL as INVALID_REASON
    ,CAST(PLAN AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ source('tuva_claim_input','pharmacy_claim') }} M