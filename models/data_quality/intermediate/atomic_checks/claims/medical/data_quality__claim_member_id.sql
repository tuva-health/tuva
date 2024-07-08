{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain 
    M.Data_SOURCE
    ,coalesce(cast(M.CLAIM_START_DATE as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID' AS DRILL_DOWN_KEY
    ,coalesce(M.CLAIM_ID, 'NULL') AS DRILL_DOWN_VALUE
    ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'MEMBER_ID' AS FIELD_NAME
    ,case when M.MEMBER_ID is not null then 'valid' else 'null' end as BUCKET_NAME
    ,cast(null as {{ dbt.type_string() }}) as INVALID_REASON
    ,CAST(Member_ID as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('medical_claim')}} M