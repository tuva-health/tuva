{{ config(
    enabled = var('clinical_enabled', False)
) }}


            SELECT
                M.Data_SOURCE
                ,coalesce(M.OBSERVATION_DATE,'1900-01-01') AS SOURCE_DATE
                ,'OBSERVATION' AS TABLE_NAME
                ,'Observation ID' as DRILL_DOWN_KEY
                ,IFNULL(OBSERVATION_ID, 'NULL') AS DRILL_DOWN_VALUE
                -- ,M.CLAIM_TYPE AS CLAIM_TYPE
                ,'OBSERVATION_DATE' AS FIELD_NAME
                ,case when M.OBSERVATION_DATE is not null then 'valid' else 'null' end as BUCKET_NAME
                ,null as INVALID_REASON
                ,CAST(OBSERVATION_DATE AS VARCHAR(255)) AS FIELD_VALUE
            FROM {{source('tuva_clinical_input','observation')}} M
            