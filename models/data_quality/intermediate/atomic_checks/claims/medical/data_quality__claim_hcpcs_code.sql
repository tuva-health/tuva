{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT 
    M.Data_SOURCE
    ,coalesce(cast(M.CLAIM_START_DATE as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID | Claim Line Number' AS DRILL_DOWN_KEY
    ,COALESCE(CAST(M.CLAIM_ID as {{ dbt.type_string() }}), 'NULL') || '|' || COALESCE(CAST(M.CLAIM_LINE_NUMBER as {{ dbt.type_string() }}), 'NULL') AS DRILL_DOWN_VALUE
    ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'HCPCS_CODE' AS FIELD_NAME
    ,case 
          when TERM.HCPCS is not null then 'valid'
          when M.HCPCS_CODE is not null then 'invalid'      
          else 'null' 
    end as BUCKET_NAME
    ,case
        when M.HCPCS_CODE is not null AND TERM.HCPCS is null then 'HCPCS does not join to Terminology HCPCS_LEVEL_2 table'
        else null
     end as INVALID_REASON
    ,CAST(M.HCPCS_CODE || '|' || COALESCE(TERM.SHORT_DESCRIPTION, '') as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
    FROM {{ ref('medical_claim')}} M
LEFT JOIN {{ ref('terminology__hcpcs_level_2')}} AS TERM ON M.HCPCS_CODE = TERM.HCPCS