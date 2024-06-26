{{ config(
    enabled = var('clinical_enabled', False)
) }}


            SELECT
                M.Data_SOURCE
                ,coalesce(M.RESULT_DATE,'1900-01-01') AS SOURCE_DATE
                ,'LAB_RESULT' AS TABLE_NAME
                ,'Lab Result ID' as DRILL_DOWN_KEY
                ,IFNULL(LAB_RESULT_ID, 'NULL') AS DRILL_DOWN_VALUE
                -- ,M.CLAIM_TYPE AS CLAIM_TYPE
                ,'NORMALIZED_DESCRIPTION' AS FIELD_NAME
                ,case when M.NORMALIZED_DESCRIPTION is not null then 'valid' else 'null' end as BUCKET_NAME
                ,null as INVALID_REASON
                ,CAST(LEFT(NORMALIZED_DESCRIPTION, 255) AS VARCHAR(255)) AS FIELD_VALUE
            FROM {{ source('tuva_clinical_input','lab_result') }} M
