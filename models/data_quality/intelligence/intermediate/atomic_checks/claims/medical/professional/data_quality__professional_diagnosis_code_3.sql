{{ config(
    enabled = var('claims_enabled', False)
) }}

WITH BASE as (
    SELECT * 
    FROM {{ source('tuva_claim_input','medical_claim') }}
    WHERE CLAIM_TYPE = 'professional'
)

SELECT
    M.Data_SOURCE
    ,coalesce(M.CLAIM_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,IFNULL(M.CLAIM_ID, 'NULL') || '|' || IFNULL(TO_VARCHAR(M.CLAIM_LINE_NUMBER), 'NULL') AS DRILL_DOWN_VALUE
    ,'professional' AS CLAIM_TYPE
    ,'DIAGNOSIS_CODE_3' AS FIELD_NAME
    ,case when TERM.ICD_10_CM is not null          then 'valid'
          WHEN M.DIAGNOSIS_CODE_3 is not null      then 'invalid'
                                                   else 'null' end as BUCKET_NAME
    ,case
        when M.DIAGNOSIS_CODE_3 is not null
            and TERM.ICD_10_CM is null
            then 'Diagnosis Code does not join to Terminology ICD_10_CM table'
        else null
    end as INVALID_REASON
    ,CAST(M.DIAGNOSIS_CODE_3 || '|' || COALESCE(TERM.LONG_DESCRIPTION, '') AS VARCHAR(255)) AS FIELD_VALUE
FROM BASE M
LEFT JOIN {{ source('tuva_terminology','icd_10_cm') }} AS TERM ON M.Diagnosis_Code_3 = TERM.ICD_10_CM