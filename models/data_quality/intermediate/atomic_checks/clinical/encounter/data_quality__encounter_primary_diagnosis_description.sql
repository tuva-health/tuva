{{ config(
    enabled = var('clinical_enabled', False)
) }}

SELECT
    M.Data_SOURCE
    ,coalesce(M.ENCOUNTER_START_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'ENCOUNTER' AS TABLE_NAME
    ,'Encounter ID' as DRILL_DOWN_KEY
    ,IFNULL(ENCOUNTER_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'PRIMARY_DIAGNOSIS_DESCRIPTION' AS FIELD_NAME
    ,case when M.PRIMARY_DIAGNOSIS_DESCRIPTION is not null then 'valid' else 'null' end as BUCKET_NAME
    ,cast(null as varchar(255)) as INVALID_REASON
    ,CAST(LEFT(PRIMARY_DIAGNOSIS_DESCRIPTION, 255) AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('encounter')}} M