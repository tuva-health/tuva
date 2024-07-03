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
    ,'NORMALIZED_CODE' AS FIELD_NAME
    ,case when TERM.icd_10_cm is not null then 'valid'
          when M.NORMALIZED_CODE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.NORMALIZED_CODE is not null and TERM.icd_10_cm is null
          then 'Normalized code does not join to Terminology icd_10_cm table'
    else null end as INVALID_REASON
    ,CAST(NORMALIZED_CODE AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('condition')}} M
LEFT JOIN {{ ref('terminology__icd_10_cm')}} TERM on m.NORMALIZED_CODE = term.icd_10_cm