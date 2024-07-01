{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain 
    M.Data_SOURCE
    ,coalesce(cast(M.ENROLLMENT_START_DATE as varchar(50)),cast('1900-01-01' as varchar(10))) AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID | Enrollment Start Date' AS DRILL_DOWN_KEY
    ,coalesce(M.Member_ID, 'NULL') as DRILL_DOWN_VALUE
    ,'ELIGIBILITY' AS CLAIM_TYPE
    ,'RACE' AS FIELD_NAME
    ,case when M.RACE is  null then 'null' 
          when R.description is  null then 'invalid'
                             else 'valid' end as BUCKET_NAME
    ,case
        when M.RACE is not null and R.description is null then 'Race does not join to terminology race table'
        else null
    end as INVALID_REASON
    ,CAST(RACE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('eligibility')}} M
LEFT JOIN {{ ref('terminology__race')}} R on M.RACE=R.description