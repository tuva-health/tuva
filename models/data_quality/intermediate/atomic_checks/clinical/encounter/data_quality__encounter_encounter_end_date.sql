{{ config(
    enabled = var('clinical_enabled', False)
) }}

SELECT
    M.Data_SOURCE
    ,coalesce(M.ENCOUNTER_START_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'ENCOUNTER' AS TABLE_NAME
    ,'Encounter ID' as DRILL_DOWN_KEY
    , coalesce(encounter_id, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'ENCOUNTER_END_DATE' AS FIELD_NAME
    ,CASE 
        WHEN M.ENCOUNTER_END_DATE > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'invalid'
        WHEN M.ENCOUNTER_END_DATE <= cast('1901-01-01' as date) THEN 'invalid'
        WHEN M.ENCOUNTER_END_DATE < M.ENCOUNTER_START_DATE THEN 'invalid'
        WHEN M.ENCOUNTER_END_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN M.ENCOUNTER_END_DATE > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'future'
        WHEN M.ENCOUNTER_END_DATE <= cast('1901-01-01' as date) THEN 'too old'
        WHEN M.ENCOUNTER_END_DATE < M.ENCOUNTER_START_DATE THEN 'Encounter end date before encounter start date'
        else null
    END AS INVALID_REASON
    ,CAST(ENCOUNTER_END_DATE as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('encounter')}} M