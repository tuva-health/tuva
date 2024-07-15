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
    ,'SOURCE_CODE' AS FIELD_NAME
    ,case when M.SOURCE_CODE is not null then 'valid' else 'null' end as BUCKET_NAME
    ,cast(null as {{ dbt.type_string() }}) as INVALID_REASON
    ,CAST(SOURCE_CODE as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('condition')}} M