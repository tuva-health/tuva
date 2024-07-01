{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.RESULT_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'LAB_RESULT' AS TABLE_NAME
    ,'Lab Result ID' as DRILL_DOWN_KEY
    ,IFNULL(LAB_RESULT_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'NORMALIZED_COMPONENT' AS FIELD_NAME
    ,case when TERM.loinc is not null then 'valid'
          when M.NORMALIZED_COMPONENT is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.NORMALIZED_COMPONENT is not null and TERM.loinc is null
          then 'Normalized component does not join to Terminology loinc table'
    else null end as INVALID_REASON
    ,CAST(NORMALIZED_COMPONENT AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('lab_result')}} M
LEFT JOIN {{ ref('terminology__loinc')}} TERM on m.NORMALIZED_COMPONENT = term.loinc