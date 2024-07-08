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
    ,'ADMIT_TYPE_CODE' AS FIELD_NAME
    ,case when TERM.ADMIT_TYPE_CODE is not null then 'valid'
          when M.ADMIT_TYPE_CODE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.ADMIT_TYPE_CODE is not null and TERM.ADMIT_TYPE_CODE is null
          then 'Admit Type Code does not join to Terminology admit_type table'
          else null end as INVALID_REASON
    ,CAST(m.ADMIT_TYPE_CODE as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('encounter')}} M
LEFT JOIN {{ ref('terminology__admit_type')}} TERM on M.ADMIT_TYPE_CODE = TERM.ADMIT_TYPE_CODE