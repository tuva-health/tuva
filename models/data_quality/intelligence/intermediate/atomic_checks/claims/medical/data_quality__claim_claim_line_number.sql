{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT
    M.Data_SOURCE
    ,coalesce(M.CLAIM_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID' as DRILL_DOWN_KEY
    ,IFNULL(CLAIM_ID, 'NULL') AS DRILL_DOWN_VALUE
    ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'CLAIM_LINE_NUMBER' AS FIELD_NAME
    ,case when M.CLAIM_LINE_NUMBER is not null then 'valid' else 'null' end as BUCKET_NAME
    ,null as INVALID_REASON
    ,CAST(Member_ID AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ source('tuva_claim_input','medical_claim') }} M