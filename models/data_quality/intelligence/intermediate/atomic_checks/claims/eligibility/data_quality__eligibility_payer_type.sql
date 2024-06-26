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
    ,'PAYER_TYPE' AS FIELD_NAME
    ,case when M.PAYER_TYPE is  null then 'null' 
          when TERM.PAYER_TYPE is null then 'invalid'
                             else 'valid' end as BUCKET_NAME
    ,case
        when M.PAYER_TYPE is not null and TERM.PAYER_TYPE is null then 'Payer Type does not join to Terminology Payer Type table'
        else null
    end as INVALID_REASON
    ,CAST(M.PAYER_TYPE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('intelligence__stg_eligibility') }} M
LEFT JOIN {{ source('tuva_terminology','payer_type') }} TERM ON M.PAYER_TYPE = TERM.PAYER_TYPE