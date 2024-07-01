{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT 
    M.Data_SOURCE
    ,coalesce(cast(M.ENROLLMENT_START_DATE as varchar(50)),cast('1900-01-01' as varchar(10))) AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID | Enrollment Start Date' AS DRILL_DOWN_KEY
    ,coalesce(M.Member_ID, 'NULL') as DRILL_DOWN_VALUE
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
FROM {{ ref('eligibility')}} M
LEFT JOIN {{ ref('terminology__medicare_status')}} TERM ON M.MEDICARE_STATUS_CODE = TERM.MEDICARE_STATUS_CODE