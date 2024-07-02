{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(GETDATE(),cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'PATIENT' AS TABLE_NAME
    ,'Patient ID' as DRILL_DOWN_KEY
    , coalesce(patient_id, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'SEX' AS FIELD_NAME
    ,case when TERM.gender is not null then 'valid'
          when M.sex is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.sex is not null and TERM.gender is null
          then 'Sex does not join to Terminology gender table'
    else null end as INVALID_REASON
    ,CAST(SEX AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('patient')}} M
LEFT JOIN {{ ref('terminology__gender')}} TERM on m.sex = term.gender