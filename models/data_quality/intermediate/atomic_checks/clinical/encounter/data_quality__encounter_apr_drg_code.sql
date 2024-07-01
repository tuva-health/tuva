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
    ,'APR_DRG_CODE' AS FIELD_NAME
    ,case when TERM.APR_DRG_CODE is not null then 'valid'
          when M.APR_DRG_CODE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.APR_DRG_CODE is not null and TERM.APR_DRG_CODE is null
          then 'APR DRG Code does not join to Terminology apr_drg table'
          else null end as INVALID_REASON
    ,CAST(m.APR_DRG_CODE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('encounter')}} M
LEFT JOIN {{ ref('terminology__apr_drg')}} TERM on m.APR_DRG_CODE = TERM.APR_DRG_CODE