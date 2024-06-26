{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT
    M.Data_SOURCE
    ,coalesce(M.ENROLLMENT_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID' AS DRILL_DOWN_KEY
    ,IFNULL(M.MEMBER_ID, 'NULL') AS DRILL_DOWN_VALUE
    ,'ELIGIBILITY' AS CLAIM_TYPE
    ,'DEATH_FLAG' AS FIELD_NAME
    ,CASE 
    
        WHEN M.DEATH_FLAG in (1,0) THEN 'valid'
        WHEN M.DEATH_FLAG is null then 'null'
        ELSE 'invalid'
        END AS BUCKET_NAME
    ,null as INVALID_REASON
    ,CAST(Death_Flag AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ source('tuva_claim_input','eligibility') }} M