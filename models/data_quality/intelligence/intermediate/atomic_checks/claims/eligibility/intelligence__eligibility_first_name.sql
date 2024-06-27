{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT 
    M.Data_SOURCE
    ,coalesce(M.ENROLLMENT_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID | Enrollment Start Date' AS DRILL_DOWN_KEY
    ,IFNULL(M.MEMBER_ID,'NULL') || '|' || IFNULL(TO_VARCHAR(M.ENROLLMENT_START_DATE), 'NULL') AS DRILL_DOWN_VALUE
    ,'ELIGIBILITY' AS CLAIM_TYPE
    ,'FIRST_NAME' AS FIELD_NAME
    ,case when M.FIRST_NAME is null then 'null' 
                             else 'valid' end as BUCKET_NAME
    ,null as INVALID_REASON
    ,CAST(FIRST_NAME AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('intelligence__stg_eligibility') }} M