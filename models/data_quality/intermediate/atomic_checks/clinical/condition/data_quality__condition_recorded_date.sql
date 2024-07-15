{{ config(
    enabled = var('clinical_enabled', False)
) }}

SELECT
    M.Data_SOURCE
    ,coalesce(M.RECORDED_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'CONDITION' AS TABLE_NAME
    ,'Condition ID' as DRILL_DOWN_KEY
    , coalesce(condition_id, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'RECORDED_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.RECORDED_DATE > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'invalid'
        WHEN M.RECORDED_DATE <= cast('1901-01-01' as date) THEN 'invalid'
        WHEN M.RECORDED_DATE < M.ONSET_DATE THEN 'invalid'
        WHEN M.RECORDED_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.RECORDED_DATE > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'future'
        WHEN M.RECORDED_DATE <= cast('1901-01-01' as date) THEN 'too old'
        WHEN M.RECORDED_DATE < M.ONSET_DATE THEN 'Recorded date before onset date'
        else null
    END AS INVALID_REASON
    ,CAST(RECORDED_DATE as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('condition')}} M