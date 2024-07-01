{{ config(
    enabled = var('clinical_enabled', False)
) }}


            SELECT
                M.Data_SOURCE
                ,coalesce(M.RESULT_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
                ,'LAB_RESULT' AS TABLE_NAME
                ,'Lab Result ID' as DRILL_DOWN_KEY
                ,IFNULL(LAB_RESULT_ID, 'NULL') AS DRILL_DOWN_VALUE
                -- ,M.CLAIM_TYPE AS CLAIM_TYPE
                ,'PATIENT_ID' AS FIELD_NAME
                ,case when M.PATIENT_ID is not null then 'valid' else 'null' end as BUCKET_NAME
                ,cast(null as varchar(255)) as INVALID_REASON
                ,CAST(PATIENT_ID AS VARCHAR(255)) AS FIELD_VALUE
            FROM {{ ref('lab_result')}} M
            