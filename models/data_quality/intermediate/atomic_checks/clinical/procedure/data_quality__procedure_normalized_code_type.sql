{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.PROCEDURE_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'PROCEDURE' AS TABLE_NAME
    ,'Procedure ID' as DRILL_DOWN_KEY
    ,IFNULL(PROCEDURE_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'NORMALIZED_CODE_TYPE' AS FIELD_NAME
    ,case when TERM.CODE_TYPE is not null then 'valid'
          when M.NORMALIZED_CODE_TYPE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.NORMALIZED_CODE_TYPE is not null and TERM.CODE_TYPE is null
          then 'Normalized code type does not join to Terminology code_type table'
    else null end as INVALID_REASON
    ,CAST(NORMALIZED_CODE_TYPE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('procedure')}} M
LEFT JOIN {{ ref('reference_data__code_type')}} TERM on m.NORMALIZED_CODE_TYPE = TERM.CODE_TYPE