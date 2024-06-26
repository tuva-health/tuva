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
    ,'MEDICARE_STATUS_CODE' AS FIELD_NAME
    ,case when M.MEDICARE_STATUS_CODE is null then 'null' 
          when TERM.MEDICARE_STATUS_CODE is null then 'invalid'
                             else 'valid' end as BUCKET_NAME
    ,case
        when M.MEDICARE_STATUS_CODE is not null and TERM.MEDICARE_STATUS_CODE is null then 'Medicare Status Code does not join to Terminology Medicare Status table'
        else null
    end as INVALID_REASON
    ,CAST(M.MEDICARE_STATUS_CODE || '|' || COALESCE(TERM.MEDICARE_STATUS_DESCRIPTION,'') AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('intelligence__stg_eligibility') }} M
LEFT JOIN {{ source('tuva_terminology','medicare_status') }} TERM ON M.MEDICARE_STATUS_CODE = TERM.MEDICARE_STATUS_CODE