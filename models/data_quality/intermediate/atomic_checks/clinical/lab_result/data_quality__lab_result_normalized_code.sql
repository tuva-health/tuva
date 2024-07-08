{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.RESULT_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'LAB_RESULT' AS TABLE_NAME
    ,'Lab Result ID' as DRILL_DOWN_KEY
    , coalesce(lab_result_id, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'NORMALIZED_CODE' AS FIELD_NAME
    ,case when TERM.loinc is not null then 'valid'
          when M.NORMALIZED_CODE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.NORMALIZED_CODE is not null and TERM.loinc is null
          then 'Normalized code does not join to Terminology loinc table'
    else null end as INVALID_REASON
    ,CAST(NORMALIZED_CODE as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('lab_result')}} M
LEFT JOIN {{ ref('terminology__loinc')}} term on m.NORMALIZED_CODE = term.loinc