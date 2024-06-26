{{ config(
    enabled = var('clinical_enabled', False)
) }}


            SELECT
                M.Data_SOURCE
                ,coalesce(GETDATE(),'1900-01-01') AS SOURCE_DATE
                ,'LOCATION' AS TABLE_NAME
                ,'Location ID' as DRILL_DOWN_KEY
                ,IFNULL(LOCATION_ID, 'NULL') AS DRILL_DOWN_VALUE
                -- ,M.CLAIM_TYPE AS CLAIM_TYPE
                ,'LOCATION_ID' AS FIELD_NAME
                ,case when M.LOCATION_ID is not null then 'valid' else 'null' end as BUCKET_NAME
                ,null as INVALID_REASON
                ,CAST(LOCATION_ID AS VARCHAR(255)) AS FIELD_VALUE
            FROM {{source('tuva_clinical_input','location')}} M
            