{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain 
    M.Data_SOURCE
    ,coalesce(cast(M.CLAIM_START_DATE as varchar(50)),cast('1900-01-01' as varchar(10))) AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID' as DRILL_DOWN_KEY
    ,coalesce(M.CLAIM_ID, 'NULL') AS DRILL_DOWN_VALUE
    ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'DIAGNOSIS_CODE_TYPE' AS FIELD_NAME
    ,case when M.DIAGNOSIS_CODE_TYPE is null then 'null' 
          when TERM.CODE_TYPE is null then 'invalid'
                             else 'valid' end as BUCKET_NAME
    ,case
        when M.DIAGNOSIS_CODE_TYPE is not null and TERM.CODE_TYPE is null then 'Diagnosis Code Type does not join to Terminology Code Type table'
        else null
    end as INVALID_REASON
    ,CAST(M.DIAGNOSIS_CODE_TYPE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('medical_claim')}} M
LEFT JOIN {{ ref('reference_data__code_type')}} TERM ON M.DIAGNOSIS_CODE_TYPE = TERM.CODE_TYPE