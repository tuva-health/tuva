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
    ,'GENDER' AS FIELD_NAME
    ,case when M.GENDER is  null then 'null' 
          when TERM.GENDER is null then 'invalid'
                             else 'valid' end as BUCKET_NAME
    ,case
        when M.GENDER is not null and TERM.GENDER is null then 'Gender does not join to Terminology Gender table'
        else null
    end as INVALID_REASON
    ,CAST(M.GENDER AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ source('tuva_claim_input','eligibility') }} M
LEFT JOIN {{ source('tuva_terminology','gender') }} TERM ON M.GENDER = TERM.GENDER