{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT
    M.Data_SOURCE
    ,coalesce(cast(M.ENROLLMENT_START_DATE as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID' AS DRILL_DOWN_KEY
    ,coalesce(M.Member_ID, 'NULL') as drill_down_value
    ,'ELIGIBILITY' AS CLAIM_TYPE
    ,'DEATH_FLAG' AS FIELD_NAME
    ,CASE
        WHEN M.DEATH_FLAG in ('1','0') THEN 'valid'
        WHEN M.DEATH_FLAG is null then 'null'
        ELSE 'invalid'
        END AS BUCKET_NAME
    ,cast(null as {{ dbt.type_string() }}) as INVALID_REASON
    ,CAST(Death_Flag as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('eligibility')}} M