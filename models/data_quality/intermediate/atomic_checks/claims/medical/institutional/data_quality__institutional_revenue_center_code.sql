{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT 
    M.Data_SOURCE
    ,coalesce(cast(M.CLAIM_START_DATE as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,COALESCE(CAST(M.CLAIM_ID as {{ dbt.type_string() }}), 'NULL') || '|' || COALESCE(CAST(M.CLAIM_LINE_NUMBER as {{ dbt.type_string() }}), 'NULL') AS DRILL_DOWN_VALUE
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
    ,CAST(M.REVENUE_CENTER_CODE || '|' || TERM.REVENUE_CENTER_DESCRIPTION as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
    FROM {{ ref('medical_claim')}} M
LEFT JOIN {{ ref('terminology__revenue_center')}} AS TERM ON M.REVENUE_CENTER_CODE = TERM.REVENUE_CENTER_CODE