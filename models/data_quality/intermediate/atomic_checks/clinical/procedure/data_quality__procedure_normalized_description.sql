{{ config(
    enabled = var('clinical_enabled', False)
) }}


            SELECT
                M.Data_SOURCE
                ,coalesce(M.PROCEDURE_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
                ,'PROCEDURE' AS TABLE_NAME
                ,'Procedure ID' as DRILL_DOWN_KEY
                , coalesce(procedure_id, 'NULL') AS DRILL_DOWN_VALUE
                -- ,M.CLAIM_TYPE AS CLAIM_TYPE
                ,'NORMALIZED_DESCRIPTION' AS FIELD_NAME
                ,case when M.NORMALIZED_DESCRIPTION is not null then 'valid' else 'null' end as BUCKET_NAME
                ,cast(null as {{ dbt.type_string() }}) as INVALID_REASON
                ,cast(substring(NORMALIZED_DESCRIPTION, 1, 255) as {{ dbt.type_string() }}) AS FIELD_VALUE
                , '{{ var('tuva_last_run')}}' as tuva_last_run
            FROM {{ ref('procedure')}} M
