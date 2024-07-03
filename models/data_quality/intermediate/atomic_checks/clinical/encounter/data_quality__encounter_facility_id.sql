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
    ,'FACILITY_ID' AS FIELD_NAME
    ,case when TERM.NPI is not null then 'valid'
          when M.FACILITY_id is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.FACILITY_id is not null and TERM.NPI is null
          then 'Facility NPI does not join to Terminology provider table'
          else null end as INVALID_REASON
    ,CAST(FACILITY_id AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('encounter')}} M
LEFT JOIN {{ ref('terminology__provider')}} TERM on m.FACILITY_id = TERM.NPI