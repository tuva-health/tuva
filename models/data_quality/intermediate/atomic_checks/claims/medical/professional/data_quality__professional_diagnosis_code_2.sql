{{ config(
    enabled = var('claims_enabled', False)
) }}

WITH BASE as (
    SELECT * 
    FROM {{ ref('medical_claim')}}
    WHERE CLAIM_TYPE = 'professional'
)

SELECT
    M.Data_SOURCE
    ,coalesce(cast(M.CLAIM_START_DATE as varchar(50)),cast('1900-01-01' as varchar(10))) AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,CONCAT(COALESCE(CAST(M.CLAIM_ID AS VARCHAR), 'NULL'),'|',COALESCE(CAST(M.CLAIM_LINE_NUMBER AS VARCHAR), 'NULL')) AS DRILL_DOWN_VALUE
    ,'professional' AS CLAIM_TYPE
    ,'DIAGNOSIS_CODE_2' AS FIELD_NAME
    ,case when TERM.ICD_10_CM is not null          then 'valid'
          WHEN M.DIAGNOSIS_CODE_2 is not null      then 'invalid'
                                                   else 'null' end as BUCKET_NAME
    ,case
        when M.DIAGNOSIS_CODE_2 is not null
            and TERM.ICD_10_CM is null
            then 'Diagnosis Code does not join to Terminology ICD_10_CM table'
        else null
    end as INVALID_REASON
    ,CAST(M.DIAGNOSIS_CODE_2 || '|' || COALESCE(term.short_description, '') AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM BASE M
LEFT JOIN {{ ref('terminology__icd_10_cm')}} AS TERM ON M.Diagnosis_Code_2 = TERM.ICD_10_CM