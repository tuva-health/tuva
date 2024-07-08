{{ config(
    enabled = var('claims_enabled', False)
) }}

with tuva_last_run as(

    select cast(substring('{{ var('tuva_last_run') }}',1,10) as date) as tuva_last_run

)
SELECT  
    M.Data_SOURCE
    ,coalesce(cast(M.CLAIM_START_DATE as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,COALESCE(CAST(M.CLAIM_ID as {{ dbt.type_string() }}), 'NULL') || '|' || COALESCE(CAST(M.CLAIM_LINE_NUMBER as {{ dbt.type_string() }}), 'NULL') AS DRILL_DOWN_VALUE
    ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'CLAIM_LINE_START_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.CLAIM_LINE_START_DATE > tuva_last_run THEN 'invalid'
        WHEN M.CLAIM_LINE_START_DATE < {{ dbt.dateadd(datepart="year", interval=-10, from_date_or_timestamp = "cte.tuva_last_run") }} THEN 'invalid'
        WHEN M.CLAIM_LINE_START_DATE < M.CLAIM_START_DATE THEN 'invalid'
        WHEN M.CLAIM_LINE_START_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.CLAIM_LINE_START_DATE > tuva_last_run THEN 'future'
        WHEN M.CLAIM_LINE_START_DATE < {{ dbt.dateadd(datepart="year", interval=-10, from_date_or_timestamp = "cte.tuva_last_run" ) }} THEN 'too old'
        WHEN M.CLAIM_LINE_START_DATE < M.CLAIM_START_DATE THEN 'line date less than than claim date'
        else null
    END AS INVALID_REASON
    ,CAST(CLAIM_LINE_START_DATE as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('medical_claim')}} M
cross join tuva_last_run cte