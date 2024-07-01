{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(GETDATE(),cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'PATIENT' AS TABLE_NAME
    ,'Patient ID' as DRILL_DOWN_KEY
    ,IFNULL(PATIENT_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'RACE' AS FIELD_NAME
    ,case when TERM.description is not null then 'valid'
          when M.race is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.race is not null and TERM.description is null
          then 'Race description does not join to Terminology race table'
    else null end as INVALID_REASON
    ,CAST(RACE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('patient')}} M
LEFT JOIN {{ ref('terminology__race')}} TERM on m.race = term.description