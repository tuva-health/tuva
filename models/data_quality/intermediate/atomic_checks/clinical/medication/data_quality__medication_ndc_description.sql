{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.DISPENSING_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'MEDICATION' AS TABLE_NAME
    ,'Medication ID' as DRILL_DOWN_KEY
    , coalesce(medication_id, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'NDC_DESCRIPTION' AS FIELD_NAME
    ,case when TERM.NDC is not null then 'valid'
          when M.NDC_CODE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.NDC_CODE is not null and TERM.NDC is null
          then 'NDC code type does not join to Terminology ndc table'
    else null end as INVALID_REASON
    ,CAST(SUBSTRING(NDC_DESCRIPTION, 1, 255) AS {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('medication')}} M
LEFT JOIN {{ ref('terminology__ndc')}} TERM on M.NDC_CODE = TERM.NDC