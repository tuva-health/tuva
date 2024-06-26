{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT 
    M.Data_SOURCE
    ,coalesce(M.ENROLLMENT_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID | Enrollment Start Date' AS DRILL_DOWN_KEY
    ,IFNULL(M.MEMBER_ID,'NULL') || '|' || IFNULL(TO_VARCHAR(M.ENROLLMENT_START_DATE), 'NULL') AS DRILL_DOWN_VALUE
    ,'ELIGIBILITY' AS CLAIM_TYPE
    ,'ORIGINAL_REASON_ENTITLEMENT_CODE' AS FIELD_NAME
    ,case when M.original_reason_entitlement_code is null then 'null' 
          when TERM.original_reason_entitlement_code is null then 'invalid'
                             else 'valid' end as BUCKET_NAME
    ,case
        when M.original_reason_entitlement_code is not null and TERM.original_reason_entitlement_code is null then 'Original Reason Entitlement Code does not join to Terminology Original Reason Entitlement Code table'
        else null
    end as INVALID_REASON
    ,CAST(M.original_reason_entitlement_code || '|' || COALESCE(TERM.ORIGINAL_REASON_ENTITLEMENT_DESCRIPTION, '') AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ source('tuva_claim_input','eligibility') }} M
LEFT JOIN {{ source('tuva_terminology','medicare_orec') }} TERM ON M.original_reason_entitlement_code = TERM.original_reason_entitlement_code