{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.RESULT_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'LAB_RESULT' AS TABLE_NAME
    ,'Lab Result ID' as DRILL_DOWN_KEY
    , coalesce(lab_result_id, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'ORDERING_PRACTITIONER_ID' AS FIELD_NAME
    ,case when TERM.NPI is not null then 'valid'
          when M.ORDERING_PRACTITIONER_ID is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.ORDERING_PRACTITIONER_ID is not null and TERM.NPI is null
          then 'Ordering practitioner ID does not join to Terminology provider table'
    else null end as INVALID_REASON
    ,CAST(ORDERING_PRACTITIONER_ID as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('lab_result')}} M
LEFT JOIN {{ ref('terminology__provider')}} TERM on m.ORDERING_PRACTITIONER_ID = TERM.NPI