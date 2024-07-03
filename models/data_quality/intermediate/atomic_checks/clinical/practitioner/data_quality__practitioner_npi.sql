{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(GETDATE(),cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'PRACTITIONER' AS TABLE_NAME
    ,'Practitioner ID' as DRILL_DOWN_KEY
    , coalesce(practitioner_id, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'NPI' AS FIELD_NAME
    ,case when TERM.npi is not null then 'valid'
          when M.npi is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.npi is not null and TERM.npi is null
          then 'NPI does not join to Terminology provider table'
    else null end as INVALID_REASON
    ,CAST(m.NPI AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('practitioner')}} M
LEFT JOIN {{ ref('terminology__provider')}} TERM ON m.npi = term.npi