{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT
    M.Data_SOURCE
    ,coalesce(cast(M.ENROLLMENT_START_DATE as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID' AS DRILL_DOWN_KEY
    ,coalesce(M.Member_ID, 'NULL') as drill_down_value
    ,'ELIGIBILITY' AS CLAIM_TYPE
    ,'ENROLLMENT_START_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.ENROLLMENT_START_DATE > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'invalid'
        WHEN M.ENROLLMENT_START_DATE <= cast('1901-01-01' as date) THEN 'invalid' 
        WHEN M.ENROLLMENT_START_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.ENROLLMENT_START_DATE > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'future'
        WHEN M.ENROLLMENT_START_DATE <= cast('1901-01-01' as date) THEN 'too old' 
    else null
    END AS INVALID_REASON
    ,CAST(ENROLLMENT_START_DATE as {{ dbt.type_string() }}) AS FIELD_VALUE
, '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('eligibility')}} M