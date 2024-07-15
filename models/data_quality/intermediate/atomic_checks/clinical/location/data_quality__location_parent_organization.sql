{{ config(
    enabled = var('clinical_enabled', False)
) }}


            SELECT
                M.Data_SOURCE
                ,coalesce(current_date,cast('1900-01-01' as date)) AS SOURCE_DATE
                ,'LOCATION' AS TABLE_NAME
                ,'Location ID' as DRILL_DOWN_KEY
                , coalesce(location_id, 'NULL') AS DRILL_DOWN_VALUE
                -- ,M.CLAIM_TYPE AS CLAIM_TYPE
                ,'PARENT_ORGANIZATION' AS FIELD_NAME
                ,case when M.PARENT_ORGANIZATION is not null then 'valid' else 'null' end as BUCKET_NAME
                ,cast(null as {{ dbt.type_string() }}) as INVALID_REASON
                ,CAST(PARENT_ORGANIZATION as {{ dbt.type_string() }}) AS FIELD_VALUE
                , '{{ var('tuva_last_run')}}' as tuva_last_run
            FROM {{ ref('location')}} M
            