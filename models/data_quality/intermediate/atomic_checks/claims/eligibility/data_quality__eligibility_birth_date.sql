{{ config(
    enabled = var('claims_enabled', False)
) }}

WITH BASE as (
    SELECT * 
    FROM {{ ref('eligibility')}}

),
UNIQUE_FIELD as (
    SELECT DISTINCT MEMBER_ID
        ,cast(BIRTH_DATE as {{ dbt.type_string() }}) as Field
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
    {{ dbt.listagg(measure="coalesce(Field, 'null')", delimiter_text="', '", order_by_clause="order by Field desc") }} AS FIELD_AGGREGATED
FROM
    UNIQUE_FIELD
GROUP BY
    DATA_SOURCE,
    MEMBER_ID
)
SELECT DISTINCT
    M.Data_SOURCE
    ,coalesce(cast(M.ENROLLMENT_START_DATE as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) AS SOURCE_DATE
    ,'ELIGIBILITY' AS TABLE_NAME
    ,'Member ID' AS DRILL_DOWN_KEY
    ,coalesce(M.Member_ID, 'NULL') as DRILL_DOWN_VALUE
    ,'ELIGIBILITY' AS CLAIM_TYPE
    ,'BIRTH_DATE' AS FIELD_NAME
    ,CASE 
        WHEN CG.FREQUENCY > 1 THEN 'multiple'      
        WHEN M.BIRTH_DATE > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'invalid'
        WHEN M.BIRTH_DATE <= cast('1901-01-01' as date) THEN 'invalid'
        WHEN M.BIRTH_DATE IS NULL THEN 'null'
        ELSE 'valid' 
    END AS BUCKET_NAME
    ,CASE 
        WHEN CG.FREQUENCY > 1 THEN 'multiple'      
        WHEN M.BIRTH_DATE > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'future'
        WHEN M.BIRTH_DATE <= cast('1901-01-01' as date) THEN 'too old'
        else null
    END AS INVALID_REASON
,CAST({{ substring('AGG.FIELD_AGGREGATED', 1, 255) }} as {{ dbt.type_string() }}) AS FIELD_VALUE
, '{{ var('tuva_last_run')}}' as tuva_last_run
FROM BASE M
LEFT JOIN CLAIM_GRAIN CG ON M.MEMBER_ID = CG.MEMBER_ID AND M.Data_Source = CG.Data_Source
LEFT JOIN CLAIM_AGG AGG ON M.MEMBER_ID = AGG.MEMBER_ID AND M.Data_Source = AGG.Data_Source

