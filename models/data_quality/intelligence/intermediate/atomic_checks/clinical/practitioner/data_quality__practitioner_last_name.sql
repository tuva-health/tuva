{{ config(
    enabled = var('clinical_enabled', False)
) }}


            SELECT
                M.Data_SOURCE
                ,coalesce(GETDATE(),'1900-01-01') AS SOURCE_DATE
                ,'PRACTITIONER' AS TABLE_NAME
                ,'Practitioner ID' as DRILL_DOWN_KEY
                ,IFNULL(PRACTITIONER_ID, 'NULL') AS DRILL_DOWN_VALUE
                -- ,M.CLAIM_TYPE AS CLAIM_TYPE
                ,'LAST_NAME' AS FIELD_NAME
                ,case when M.LAST_NAME is not null then 'valid' else 'null' end as BUCKET_NAME
                ,null as INVALID_REASON
                ,CAST(LAST_NAME AS VARCHAR(255)) AS FIELD_VALUE
            FROM {{source('tuva_clinical_input','practitioner')}} M
            