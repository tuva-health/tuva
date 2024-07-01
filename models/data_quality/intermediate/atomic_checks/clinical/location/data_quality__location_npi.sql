{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(GETDATE(),cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'LOCATION' AS TABLE_NAME
    ,'Location ID' as DRILL_DOWN_KEY
    ,IFNULL(LOCATION_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'NPI' AS FIELD_NAME
    ,case when TERM.NPI is not null then 'valid'
          when M.NPI is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.NPI is not null and TERM.NPI is null
          then 'NPI does not join to Terminology provider table'
    else null end as INVALID_REASON
    ,CAST(m.NPI AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('location')}} M
LEFT JOIN {{ ref('terminology__provider')}} TERM on m.NPI = term.NPI