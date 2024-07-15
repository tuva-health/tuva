{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT  
    M.Data_SOURCE
    ,coalesce(cast(M.PAID_DATE as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) AS SOURCE_DATE
    ,'PHARMACY_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,COALESCE(CAST(M.CLAIM_ID as {{ dbt.type_string() }}), 'NULL') || '|' || COALESCE(CAST(M.CLAIM_LINE_NUMBER as {{ dbt.type_string() }}), 'NULL') AS DRILL_DOWN_VALUE
    ,'PHARMACY' AS CLAIM_TYPE
    ,'DATA_SOURCE' AS FIELD_NAME
    ,CASE 
        WHEN M.DATA_SOURCE is null then 'null' else 'valid' END AS BUCKET_NAME
    ,cast(null as {{ dbt.type_string() }}) as INVALID_REASON
    ,CAST(DATA_SOURCE as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('pharmacy_claim')}} M