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
    ,'ENCOUNTER_TYPE' AS FIELD_NAME
    ,case when TERM.encounter_type is not null then 'valid'
          when M.encounter_type is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.encounter_type is not null and TERM.encounter_type is null
          then 'Encounter type does not join to Terminology encounter_type table'
          else null end as INVALID_REASON
    ,CAST(m.ENCOUNTER_TYPE AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('encounter')}} M
LEFT JOIN {{ ref('terminology__encounter_type')}} TERM on m.encounter_type = term.encounter_type