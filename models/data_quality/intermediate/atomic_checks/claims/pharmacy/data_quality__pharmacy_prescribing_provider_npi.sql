{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain 
    M.Data_SOURCE
    ,coalesce(cast(M.PAID_DATE as varchar(50)),cast('1900-01-01' as varchar(10))) AS SOURCE_DATE
    ,'PHARMACY_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,CONCAT(COALESCE(CAST(M.CLAIM_ID AS VARCHAR), 'NULL'),'|',COALESCE(CAST(M.CLAIM_LINE_NUMBER AS VARCHAR), 'NULL')) AS DRILL_DOWN_VALUE
    ,'PHARMACY' AS CLAIM_TYPE
    ,'PRESCRIBING_PROVIDER_NPI' AS FIELD_NAME
    ,case when TERM.NPI is not null          then        'valid'
          when M.PRESCRIBING_PROVIDER_NPI is not null    then 'invalid'      
                                             else 'null' end as BUCKET_NAME
    ,case
        when M.PRESCRIBING_PROVIDER_NPI is not null
            and TERM.NPI is null
            then 'Prescribing Provider NPI does not join to Terminology Provider table'
        else null
    end as INVALID_REASON                                             
    ,CAST(M.PRESCRIBING_PROVIDER_NPI AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('pharmacy_claim')}} M
LEFT JOIN {{ ref('terminology__provider')}} AS TERM ON M.PRESCRIBING_PROVIDER_NPI = TERM.NPI