{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT 
    M.Data_SOURCE
    ,coalesce(M.CLAIM_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,IFNULL(M.CLAIM_ID, 'NULL') || '|' || IFNULL(TO_VARCHAR(M.CLAIM_LINE_NUMBER), 'NULL') AS DRILL_DOWN_VALUE
    ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'HCPCS_CODE' AS FIELD_NAME
    ,case 
          when TERM.HCPCS is not null then 'valid'
          when CPT.HCPCS is not null then 'valid'
          when M.HCPCS_CODE is not null then 'invalid'      
          else 'null' 
    end as BUCKET_NAME
    ,case
        when M.HCPCS_CODE is not null AND TERM.HCPCS is null AND CPT.HCPCS is null then 'HCPCS does not join to Terminology HCPCS_LEVEL_2 table'
        else null
     end as INVALID_REASON
    ,CAST(M.HCPCS_CODE || '|' || COALESCE(TERM.SHORT_DESCRIPTION, '') AS VARCHAR(255)) AS FIELD_VALUE
    FROM {{ ref('intelligence__stg_medical_claim') }} M
LEFT JOIN {{ ref('terminology__hcpcs_level_2') }} AS TERM ON M.HCPCS_CODE = TERM.HCPCS
LEFT JOIN {{ ref('terminology__cpt') }} AS CPT on M.HCPCS_CODE = CPT.HCPCS