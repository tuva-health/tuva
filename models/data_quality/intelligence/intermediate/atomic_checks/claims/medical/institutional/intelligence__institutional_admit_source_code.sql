{{ config(
    enabled = var('claims_enabled', False)
) }}

WITH BASE as (
    SELECT * 
    FROM {{ ref('intelligence__stg_medical_claim') }}
    WHERE CLAIM_TYPE = 'institutional'
        AND LEFT(bill_type_code, 2) = '11'
),
UNIQUE_FIELD as (
    SELECT DISTINCT CLAIM_ID
        ,BASE.ADMIT_SOURCE_CODE || '|' || COALESCE(TERM.ADMIT_SOURCE_DESCRIPTION, '') as Field
        ,DATA_SOURCE
    FROM BASE
    LEFT JOIN {{ ref('terminology__admit_source') }} AS TERM ON BASE.ADMIT_SOURCE_CODE = TERM.ADMIT_SOURCE_CODE
),
CLAIM_GRAIN as (
    SELECT CLAIM_ID
        ,DATA_SOURCE
        ,count(*) as FREQUENCY
    from UNIQUE_FIELD 
    GROUP BY CLAIM_ID
        ,DATA_SOURCE
),
CLAIM_AGG as (
    SELECT
        CLAIM_ID,
        DATA_SOURCE,
        LISTAGG(IFF(Field IS NULL, 'null', TO_VARCHAR(Field)), ', ') WITHIN GROUP (ORDER BY Field DESC) AS FIELD_AGGREGATED
    FROM
        UNIQUE_FIELD
    GROUP BY
        CLAIM_ID,
        DATA_SOURCE
)
SELECT DISTINCT -- to bring to claim_ID grain 
    M.Data_SOURCE
    ,coalesce(M.CLAIM_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID' AS DRILL_DOWN_KEY
    ,IFNULL(M.CLAIM_ID, 'NULL') AS DRILL_DOWN_VALUE
    ,'institutional' AS CLAIM_TYPE
    ,'ADMIT_SOURCE_CODE' AS FIELD_NAME
    ,case when CG.FREQUENCY > 1                then 'multiple'      
          when TERM.ADMIT_SOURCE_CODE is not null then 'valid'
          when M.ADMIT_SOURCE_CODE is not null    then 'invalid'      
                                               else 'null' end as BUCKET_NAME
    ,case 
        when M.ADMIT_SOURCE_CODE is not null 
            and term.ADMIT_SOURCE_CODE is null 
            and CG. FREQUENCY = 1
            then 'Admit source code does not join to Terminology Admit Source table' 
        else null
    end as INVALID_REASON
    ,CAST(LEFT(AGG.FIELD_AGGREGATED,255) AS VARCHAR(255)) AS FIELD_VALUE
FROM BASE M
LEFT JOIN CLAIM_GRAIN CG ON M.CLAIM_ID = CG.CLAIM_ID AND M.Data_Source = CG.Data_Source
LEFT JOIN {{ ref('terminology__admit_source') }} AS TERM ON M.ADMIT_SOURCE_CODE = TERM.ADMIT_SOURCE_CODE
LEFT JOIN CLAIM_AGG AGG ON M.CLAIM_ID = AGG.CLAIM_ID AND M.Data_Source = AGG.Data_Source