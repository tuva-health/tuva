{{ config(
    enabled = var('clinical_enabled', False)
) }}

SELECT
    M.Data_SOURCE
    ,coalesce(M.RECORDED_DATE,'1900-01-01') AS SOURCE_DATE
    ,'CONDITION' AS TABLE_NAME
    ,'Condition ID' as DRILL_DOWN_KEY
    ,IFNULL(CONDITION_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'PRESENT_ON_ADMIT_DESCRIPTION' AS FIELD_NAME
    ,case when M.PRESENT_ON_ADMIT_DESCRIPTION is not null then 'valid' else 'null' end as BUCKET_NAME
    ,null as INVALID_REASON
    ,CAST(LEFT(PRESENT_ON_ADMIT_DESCRIPTION, 255) AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ source('tuva_clinical_input','condition') }} M
