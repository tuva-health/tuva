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
    ,'GENDER' AS FIELD_NAME
    ,case when M.GENDER is  null then 'null' 
          when TERM.GENDER is null then 'invalid'
                             else 'valid' end as BUCKET_NAME
    ,case
        when M.GENDER is not null and TERM.GENDER is null then 'Gender does not join to Terminology Gender table'
        else null
    end as INVALID_REASON
    ,CAST(M.GENDER AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('eligibility')}} M
LEFT JOIN {{ ref('terminology__gender')}} TERM ON M.GENDER = TERM.GENDER