{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT 
    M.Data_SOURCE
    ,coalesce(M.CLAIM_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,IFNULL(M.CLAIM_ID, 'NULL') || '|' || IFNULL(TO_VARCHAR(M.CLAIM_LINE_NUMBER), 'NULL') AS DRILL_DOWN_VALUE
    ,'institutional' AS CLAIM_TYPE
    ,'REVENUE_CENTER_CODE' AS FIELD_NAME
    ,case 
          when TERM.REVENUE_CENTER_CODE is not null then 'valid'
          when M.REVENUE_CENTER_CODE is not null    then 'invalid'      
                                                    else 'null' end as BUCKET_NAME
    ,case
        when M.REVENUE_CENTER_CODE is not null
            and term.REVENUE_CENTER_CODE is null
            then 'Revenue center code does not join to Terminology Revenue Center table'
        else null
    end as INVALID_REASON                        
    ,CAST(M.REVENUE_CENTER_CODE || '|' || TERM.REVENUE_CENTER_DESCRIPTION AS VARCHAR(255)) AS FIELD_VALUE
    FROM {{ ref('intelligence__stg_medical_claim') }} M
LEFT JOIN {{ ref('terminology__revenue_center') }} AS TERM ON M.REVENUE_CENTER_CODE = TERM.REVENUE_CENTER_CODE