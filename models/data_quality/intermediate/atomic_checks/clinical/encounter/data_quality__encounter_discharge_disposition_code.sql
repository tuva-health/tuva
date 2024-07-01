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
    ,'DISCHARGE_DISPOSITION_CODE' AS FIELD_NAME
    ,case when TERM.DISCHARGE_DISPOSITION_CODE is not null then 'valid'
          when M.DISCHARGE_DISPOSITION_CODE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.DISCHARGE_DISPOSITION_CODE is not null and TERM.DISCHARGE_DISPOSITION_CODE is null
          then 'Discharge Disposition Code does not join to Terminology discharge_disposition table'
          else null end as INVALID_REASON
    ,CAST(m.DISCHARGE_DISPOSITION_CODE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('encounter')}} M
LEFT JOIN {{ ref('terminology__discharge_disposition')}} TERM ON M.DISCHARGE_DISPOSITION_CODE = TERM.DISCHARGE_DISPOSITION_CODE