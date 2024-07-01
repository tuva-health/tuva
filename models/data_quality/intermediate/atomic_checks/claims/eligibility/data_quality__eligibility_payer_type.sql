{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT 
    M.Data_SOURCE
    ,coalesce(cast(M.ENROLLMENT_START_DATE as varchar(50)),cast('1900-01-01' as varchar(10))) AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID | Enrollment Start Date' AS DRILL_DOWN_KEY
    ,coalesce(M.Member_ID, 'NULL') as DRILL_DOWN_VALUE
    ,'ELIGIBILITY' AS CLAIM_TYPE
    ,'PAYER_TYPE' AS FIELD_NAME
    ,case when M.PAYER_TYPE is  null then 'null' 
          when TERM.PAYER_TYPE is null then 'invalid'
                             else 'valid' end as BUCKET_NAME
    ,case
        when M.PAYER_TYPE is not null and TERM.PAYER_TYPE is null then 'Payer Type does not join to Terminology Payer Type table'
        else null
    end as INVALID_REASON
    ,CAST(M.PAYER_TYPE AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('eligibility')}} M
LEFT JOIN {{ ref('terminology__payer_type')}} TERM ON M.PAYER_TYPE = TERM.PAYER_TYPE