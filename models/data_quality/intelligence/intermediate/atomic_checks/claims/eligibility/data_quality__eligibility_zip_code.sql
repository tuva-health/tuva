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
    ,'ZIP_CODE' AS FIELD_NAME
    ,case when M.ZIP_CODE is  null then 'null' 
          when length(M.ZIP_CODE) in  (5,9,10) then 'valid'
                             else 'invalid' end as BUCKET_NAME
    ,case
        when m.ZIP_CODE is not null and length(m.ZIP_CODE) NOT IN (5,9,10) then 'Invalid Zip Code Length'
        else null
     end as INVALID_REASON
    ,CAST(ZIP_CODE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ source('tuva_claim_input','eligibility') }} M