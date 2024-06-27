{{ config(
    enabled = var('claims_enabled', False)
) }}

WITH BASE as (
    SELECT * 
    FROM {{ ref('intelligence__stg_medical_claim') }}
    WHERE CLAIM_TYPE = 'professional'
)

SELECT
    M.Data_SOURCE
    ,coalesce(M.CLAIM_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,IFNULL(M.CLAIM_ID, 'NULL') || '|' || IFNULL(TO_VARCHAR(M.CLAIM_LINE_NUMBER), 'NULL') AS DRILL_DOWN_VALUE
    ,'professional' AS CLAIM_TYPE
    ,'RENDERING_NPI' AS FIELD_NAME
    ,case when TERM.NPI is not null          then        'valid'
          when M.RENDERING_NPI is not null    then 'invalid'      
                                             else 'null' end as BUCKET_NAME
    ,case
        when m.Rendering_NPI is not null
            and TERM.NPI is null
            then 'Rendering Provider NPI does not join to Terminology Provider table'
        else null
    end as INVALID_REASON
    ,CAST(M.RENDERING_NPI AS VARCHAR(255)) AS FIELD_VALUE
FROM BASE M
LEFT JOIN {{ ref('terminology__provider') }} AS TERM ON M.RENDERING_NPI = TERM.NPI