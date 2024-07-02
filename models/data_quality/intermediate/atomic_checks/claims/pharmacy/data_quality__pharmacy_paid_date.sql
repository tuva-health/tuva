{{ config(
    enabled = var('claims_enabled', False)
) }}
with tuva_last_run as(
    select cast('{{ var('tuva_last_run') }}' as date) as tuva_last_run
)
SELECT  
    M.Data_SOURCE
    ,coalesce(cast(M.PAID_DATE as varchar(50)),cast('1900-01-01' as varchar(10))) AS SOURCE_DATE
    ,'PHARMACY_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,CONCAT(COALESCE(CAST(M.CLAIM_ID AS VARCHAR), 'NULL'),'|',COALESCE(CAST(M.CLAIM_LINE_NUMBER AS VARCHAR), 'NULL')) AS DRILL_DOWN_VALUE
    ,'PHARMACY' AS CLAIM_TYPE
    ,'PAID_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.PAID_DATE > '{{ var('tuva_last_run') }}' THEN 'invalid'
        WHEN M.PAID_DATE < {{ dbt.dateadd(datepart="year", interval=-10, from_date_or_timestamp="cte.tuva_last_run") }} THEN 'invalid'
        WHEN M.PAID_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.PAID_DATE > '{{ var('tuva_last_run') }}' THEN 'future'
        WHEN M.PAID_DATE < {{ dbt.dateadd(datepart="year", interval=-10, from_date_or_timestamp="cte.tuva_last_run") }} THEN 'too old'
        else null
        END AS INVALID_REASON
    ,CAST(PAID_DATE AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('pharmacy_claim')}} M
cross join tuva_last_run cte