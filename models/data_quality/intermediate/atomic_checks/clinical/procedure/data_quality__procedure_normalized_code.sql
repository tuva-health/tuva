{{ config(
    enabled = var('clinical_enabled', False)
) }}

with icd9 as (
    SELECT
        M.Data_SOURCE
        ,coalesce(M.PROCEDURE_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
        ,'PROCEDURE' AS TABLE_NAME
        ,'Procedure ID' as DRILL_DOWN_KEY
        ,IFNULL(PROCEDURE_ID, 'NULL') AS DRILL_DOWN_VALUE
        -- ,M.CLAIM_TYPE AS CLAIM_TYPE
        ,'NORMALIZED_CODE' AS FIELD_NAME
        ,case when TERM.icd_9_pcs is not null then 'valid'
            when M.NORMALIZED_CODE is not null then 'invalid'
            else 'null' 
        end as BUCKET_NAME
        ,case when M.NORMALIZED_CODE is not null and TERM.icd_9_pcs is null
            then 'Normalized code does not join to Terminology icd_9_pcs table'
        else null end as INVALID_REASON
        ,CAST(NORMALIZED_CODE AS VARCHAR(255)) AS FIELD_VALUE
    FROM {{ ref('procedure')}} M
    LEFT JOIN {{ ref('terminology__icd_9_pcs')}} TERM on m.NORMALIZED_CODE = term.icd_9_pcs
    WHERE
        m.NORMALIZED_CODE_TYPE = 'icd-9-pcs'
),
icd10 as (
    SELECT
    M.Data_SOURCE
    ,coalesce(M.PROCEDURE_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'PROCEDURE' AS TABLE_NAME
    ,'Procedure ID' as DRILL_DOWN_KEY
    ,IFNULL(PROCEDURE_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'NORMALIZED_CODE' AS FIELD_NAME
    ,case when TERM.icd_10_pcs is not null then 'valid'
          when M.NORMALIZED_CODE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.NORMALIZED_CODE is not null and TERM.icd_10_pcs is null
          then 'Normalized code does not join to Terminology icd_10_pcs table'
    else null end as INVALID_REASON
    ,CAST(NORMALIZED_CODE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('procedure')}} M
LEFT JOIN {{ ref('terminology__icd_10_pcs')}} TERM on m.NORMALIZED_CODE = term.icd_10_pcs
WHERE
    m.NORMALIZED_CODE_TYPE = 'icd_10_pcs'
),
hcpcs_level_2 as (
    SELECT
    M.Data_SOURCE
    ,coalesce(M.PROCEDURE_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'PROCEDURE' AS TABLE_NAME
    ,'Procedure ID' as DRILL_DOWN_KEY
    ,IFNULL(PROCEDURE_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'NORMALIZED_CODE' AS FIELD_NAME
    ,case when TERM.hcpcs is not null then 'valid'
          when M.NORMALIZED_CODE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.NORMALIZED_CODE is not null and TERM.hcpcs is null
          then 'Normalized code does not join to Terminology hcpcs_level_2 table'
    else null end as INVALID_REASON
    ,CAST(NORMALIZED_CODE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('procedure')}} M
LEFT JOIN {{ ref('terminology__hcpcs_level_2')}} TERM on m.NORMALIZED_CODE = term.hcpcs
WHERE
    m.NORMALIZED_CODE_TYPE = 'hcpcs_level_2'
),

others as (
    SELECT
    M.Data_SOURCE
    ,coalesce(M.PROCEDURE_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'PROCEDURE' AS TABLE_NAME
    ,'Procedure ID' as DRILL_DOWN_KEY
    ,IFNULL(PROCEDURE_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'NORMALIZED_CODE' AS FIELD_NAME
    ,'null' as BUCKET_NAME
    ,'Code Type does not have a matching code terminology table' as INVALID_REASON
    ,CAST(NORMALIZED_CODE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('procedure')}} M
WHERE
    m.NORMALIZED_CODE_TYPE NOT IN ('icd-9-pcs', 'icd-10-pcs','hcpcs_level_2')
)

SELECT *, '{{ var('tuva_last_run')}}' as tuva_last_run FROM icd9

UNION

SELECT * , '{{ var('tuva_last_run')}}' as tuva_last_run FROM icd10

UNION

SELECT * , '{{ var('tuva_last_run')}}' as tuva_last_run FROM hcpcs_level_2

UNION

SELECT * , '{{ var('tuva_last_run')}}' as tuva_last_run FROM others