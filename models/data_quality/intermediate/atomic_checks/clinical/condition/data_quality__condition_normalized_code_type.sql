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
    ,'NORMALIZED_CODE_TYPE' AS FIELD_NAME
    ,case when TERM.CODE_TYPE is not null then 'valid'
          when M.NORMALIZED_CODE_TYPE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.NORMALIZED_CODE_TYPE is not null and TERM.CODE_TYPE is null
          then 'Normalized Code Type does not join to Terminology code_type table'
          else null end as INVALID_REASON
    ,CAST(NORMALIZED_CODE_TYPE as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('condition')}} M
LEFT JOIN {{ ref('reference_data__code_type')}} AS TERM ON M.NORMALIZED_CODE_TYPE = TERM.CODE_TYPE