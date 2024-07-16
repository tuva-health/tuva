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
                ,'ATC_DESCRIPTION' AS FIELD_NAME
                ,case when M.ATC_DESCRIPTION is not null then 'valid' else 'null' end as BUCKET_NAME
                ,cast(null as {{ dbt.type_string() }}) as INVALID_REASON
                ,cast(substring(ATC_DESCRIPTION, 1, 255) as {{ dbt.type_string() }}) AS FIELD_VALUE
                , '{{ var('tuva_last_run')}}' as tuva_last_run
            FROM {{ ref('medication')}} M
