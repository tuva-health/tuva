{{ config(
    enabled = var('clinical_enabled', False)
) }}

SELECT
    M.Data_SOURCE
    ,coalesce(M.RECORDED_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'CONDITION' AS TABLE_NAME
    ,'Condition ID' as DRILL_DOWN_KEY
    ,IFNULL(CONDITION_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'PRESENT_ON_ADMIT_CODE' AS FIELD_NAME
    ,case when TERM.PRESENT_ON_ADMIT_CODE is not null then 'valid'
          when M.PRESENT_ON_ADMIT_CODE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.PRESENT_ON_ADMIT_CODE is not null and TERM.PRESENT_ON_ADMIT_CODE is null
          then 'Present On Admit Code does not join to Terminology present_on_admission table'
          else null 
    end as INVALID_REASON
    ,CAST(m.PRESENT_ON_ADMIT_CODE AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('condition')}} M
LEFT JOIN {{ ref('terminology__present_on_admission')}} TERM ON m.PRESENT_ON_ADMIT_CODE = TERM.PRESENT_ON_ADMIT_CODE