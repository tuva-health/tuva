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
    ,coalesce(cast(M.CLAIM_START_DATE as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,COALESCE(CAST(M.CLAIM_ID as {{ dbt.type_string() }}), 'NULL') || '|' || COALESCE(CAST(M.CLAIM_LINE_NUMBER as {{ dbt.type_string() }}), 'NULL') AS DRILL_DOWN_VALUE
    ,'professional' AS CLAIM_TYPE
    ,'DIAGNOSIS_CODE_1' AS FIELD_NAME
    ,case when TERM.ICD_10_CM is not null          then 'valid'
          WHEN M.DIAGNOSIS_CODE_1 is not null      then 'invalid'
                                                   else 'null' end as BUCKET_NAME
    ,case
        when M.DIAGNOSIS_CODE_1 is not null
            and TERM.ICD_10_CM is null
            then 'Diagnosis Code does not join to Terminology ICD_10_CM table'
        else null
    end as INVALID_REASON
    ,CAST(M.DIAGNOSIS_CODE_1 || '|' || COALESCE(term.short_description, '') as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM BASE M
LEFT JOIN {{ ref('terminology__icd_10_cm')}} AS TERM ON M.Diagnosis_Code_1 = TERM.ICD_10_CM