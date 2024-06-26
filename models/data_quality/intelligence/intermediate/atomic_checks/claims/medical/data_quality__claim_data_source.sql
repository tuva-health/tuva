{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain 
    M.Data_SOURCE
    ,coalesce(M.CLAIM_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID' as DRILL_DOWN_KEY
    ,IFNULL(CLAIM_ID, 'NULL') AS DRILL_DOWN_VALUE
    ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'DATA_SOURCE' AS FIELD_NAME
    ,case when M.DATA_SOURCE is not null then 'valid' else 'null' end as BUCKET_NAME
    ,null as INVALID_REASON
    ,CAST(DATA_SOURCE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('intelligence__stg_medical_claim') }} M