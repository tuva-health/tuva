{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT 
    M.Data_SOURCE
    ,coalesce(cast(M.ENROLLMENT_START_DATE as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID | Enrollment Start Date' AS DRILL_DOWN_KEY
    ,coalesce(M.Member_ID, 'NULL') as DRILL_DOWN_VALUE
    ,'ELIGIBILITY' AS CLAIM_TYPE
    ,'ORIGINAL_REASON_ENTITLEMENT_CODE' AS FIELD_NAME
    ,case when M.original_reason_entitlement_code is null then 'null' 
          when TERM.original_reason_entitlement_code is null then 'invalid'
                             else 'valid' end as BUCKET_NAME
    ,case
        when M.original_reason_entitlement_code is not null and TERM.original_reason_entitlement_code is null then 'Original Reason Entitlement Code does not join to Terminology Original Reason Entitlement Code table'
        else null
    end as INVALID_REASON
    ,CAST(M.original_reason_entitlement_code || '|' || COALESCE(TERM.ORIGINAL_REASON_ENTITLEMENT_DESCRIPTION, '') as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('eligibility')}} M
LEFT JOIN {{ ref('terminology__medicare_orec')}} TERM ON M.original_reason_entitlement_code = TERM.original_reason_entitlement_code