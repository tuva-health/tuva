{{ config(
    enabled = var('claims_enabled', False)
) }}

WITH BASE as (
    SELECT * 
    FROM {{ ref('intelligence__stg_eligibility') }}

),
UNIQUE_FIELD as (
    SELECT DISTINCT MEMBER_ID
        ,DEATH_DATE as Field
        ,DATA_SOURCE
    FROM BASE
),
CLAIM_GRAIN as (
    SELECT MEMBER_ID
        ,DATA_SOURCE
        ,count(*) as FREQUENCY
    from UNIQUE_FIELD 
    GROUP BY MEMBER_ID
        ,DATA_SOURCE
),
CLAIM_AGG as (
    SELECT
        MEMBER_ID,
        DATA_SOURCE,
        LISTAGG(IFF(Field IS NULL, 'null', TO_VARCHAR(Field)), ', ') WITHIN GROUP (ORDER BY Field DESC) AS FIELD_AGGREGATED

    FROM
        UNIQUE_FIELD
    GROUP BY
        MEMBER_ID,
        DATA_SOURCE
)
SELECT DISTINCT
    M.Data_SOURCE
    ,coalesce(M.ENROLLMENT_START_DATE,'1900-01-01') AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID' AS DRILL_DOWN_KEY
    ,IFNULL(M.MEMBER_ID, 'NULL') AS DRILL_DOWN_VALUE
    ,'ELIGIBILITY' AS CLAIM_TYPE
    ,'DEATH_DATE' AS FIELD_NAME
    ,CASE 
        WHEN CG.FREQUENCY > 1 THEN 'multiple'      
        WHEN M.DEATH_DATE > CURRENT_DATE() THEN 'invalid'
        WHEN M.DEATH_DATE <= '1901-01-01' THEN 'invalid'
        WHEN M.DEATH_DATE <= M.BIRTH_DATE THEN 'invalid'
        WHEN M.DEATH_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN CG.FREQUENCY > 1 THEN 'multiple'      
        WHEN M.DEATH_DATE > CURRENT_DATE() THEN 'future'
        WHEN M.DEATH_DATE <= '1901-01-01' THEN 'too old'
        WHEN M.DEATH_DATE <= M.BIRTH_DATE THEN 'death date before birth date'
        else null
    END AS INVALID_REASON
    ,CAST(LEFT(AGG.FIELD_AGGREGATED,255) AS VARCHAR(255)) AS FIELD_VALUE
FROM BASE M
LEFT JOIN CLAIM_GRAIN CG ON M.MEMBER_ID = CG.MEMBER_ID AND M.Data_Source = CG.Data_Source
LEFT JOIN CLAIM_AGG AGG ON M.MEMBER_ID = AGG.MEMBER_ID AND M.Data_Source = AGG.Data_Source