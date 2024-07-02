{{ config(
    enabled = var('clinical_enabled', False)
) }}


            SELECT
                M.Data_SOURCE
                ,coalesce(M.DISPENSING_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
                ,'MEDICATION' AS TABLE_NAME
                ,'Medication ID' as DRILL_DOWN_KEY
                ,IFNULL(MEDICATION_ID, 'NULL') AS DRILL_DOWN_VALUE
                -- ,M.CLAIM_TYPE AS CLAIM_TYPE
                ,'SOURCE_DESCRIPTION' AS FIELD_NAME
                ,case when M.SOURCE_DESCRIPTION is not null then 'valid' else 'null' end as BUCKET_NAME
                ,cast(null as varchar(255)) as INVALID_REASON
                ,CAST(LEFT(SOURCE_DESCRIPTION, 255) AS VARCHAR(255)) AS FIELD_VALUE
                , '{{ var('tuva_last_run')}}' as tuva_last_run
            FROM {{ ref('medication')}} M