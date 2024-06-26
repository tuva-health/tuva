{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain 
    M.Data_SOURCE
    ,coalesce(M.ENROLLMENT_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID | Enrollment Start Date' AS DRILL_DOWN_KEY
    ,IFNULL(M.MEMBER_ID,'NULL') || '|' || IFNULL(TO_VARCHAR(M.ENROLLMENT_START_DATE), 'NULL') AS DRILL_DOWN_VALUE
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
FROM {{ source('tuva_claim_input','eligibility') }} M
LEFT JOIN {{ source('tuva_terminology','race') }} R on M.RACE=R.description