{{ config(
    enabled = var('clinical_enabled', False)
) }}

SELECT
    M.Data_SOURCE
    ,coalesce(M.ENCOUNTER_START_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'ENCOUNTER' AS TABLE_NAME
    ,'Encounter ID' as DRILL_DOWN_KEY
    ,IFNULL(ENCOUNTER_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'PRIMARY_DIAGNOSIS_CODE_TYPE' AS FIELD_NAME
    ,case when TERM.CODE_TYPE is not null then 'valid'
          when M.PRIMARY_DIAGNOSIS_CODE_TYPE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.PRIMARY_DIAGNOSIS_CODE_TYPE is not null and TERM.CODE_TYPE is null
          then 'Primary Diagnosis Code Type does not join to Terminology code_type table'
          else null end as INVALID_REASON
    ,CAST(PRIMARY_DIAGNOSIS_CODE_TYPE AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('encounter')}} M
LEFT JOIN {{ ref('reference_data__code_type')}} TERM on M.PRIMARY_DIAGNOSIS_CODE_TYPE = TERM.CODE_TYPE