{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain 
    M.Data_SOURCE
    ,coalesce(M.CLAIM_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID' as DRILL_DOWN_KEY
    ,IFNULL(CLAIM_ID, 'NULL') AS DRILL_DOWN_VALUE
    ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'CLAIM_TYPE' AS FIELD_NAME
    ,case when M.CLAIM_TYPE is null then 'null' 
          when TERM.CLAIM_TYPE is null then 'invalid'
                             else 'valid' end as BUCKET_NAME
    ,case
        when M.CLAIM_TYPE is not null and TERM.CLAIM_TYPE is null then 'Claim Type does not join to Terminology Claim Type table'
        else null
    end as INVALID_REASON
    ,CAST(M.CLAIM_TYPE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('intelligence__stg_medical_claim') }} M
LEFT JOIN {{ ref('terminology__claim_type') }} TERM ON M.CLAIM_TYPE = TERM.CLAIM_TYPE