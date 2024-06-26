{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain 
    M.Data_SOURCE
    ,coalesce(M.PAID_DATE,'1900-01-01') AS SOURCE_DATE
    ,'PHARMACY_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,IFNULL(M.CLAIM_ID, 'NULL') || '|' || IFNULL(TO_VARCHAR(M.CLAIM_LINE_NUMBER), 'NULL') AS DRILL_DOWN_VALUE
    ,'PHARMACY' AS CLAIM_TYPE
    ,'NDC_CODE' AS FIELD_NAME
    ,case when TERM.NDC is not null          then        'valid'
          when M.NDC_CODE is not null        then 'invalid'      
                                             else 'null' end as BUCKET_NAME
    ,case
        when M.NDC_CODE is not null
            and TERM.NDC is null
            then 'NDC Code does not join to Terminology NDC table'
        else null
    end as INVALID_REASON
    ,CAST(LEFT(M.NDC_CODE || '|' || COALESCE(TERM.RXNORM_DESCRIPTION, TERM.FDA_DESCRIPTION, ''), 255) AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ source('tuva_claim_input','pharmacy_claim') }} M
LEFT JOIN {{ source('tuva_terminology','ndc') }} AS TERM ON M.NDC_CODE = TERM.NDC