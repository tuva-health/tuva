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
    ,'STATE' AS FIELD_NAME
    ,case when M.STATE is  null then 'null' 
          when TERM.SSA_FIPS_STATE_NAME is null then 'invalid'
                             else 'valid' end as BUCKET_NAME
    ,case
        when M.STATE is not null and TERM.SSA_FIPS_STATE_NAME is null then 'State does not join to Terminology SSA_FIPS_STATE table'
        else null
    end as INVALID_REASON
    ,CAST(STATE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('eligibility')}} M
LEFT JOIN {{ ref('reference_data__ssa_fips_state')}} AS TERM ON M.STATE = TERM.SSA_FIPS_STATE_NAME