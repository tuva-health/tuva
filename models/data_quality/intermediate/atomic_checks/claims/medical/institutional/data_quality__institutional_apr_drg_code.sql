{{ config(
    enabled = var('claims_enabled', False)
) }}

WITH BASE as (
SELECT * 
FROM {{ ref('data_quality__stg_institutional_inpatient') }}
),
UNIQUE_FIELD as (
    SELECT DISTINCT CLAIM_ID
        ,BASE.APR_DRG_CODE || '|' || COALESCE(TERM.APR_DRG_DESCRIPTION,'') as Field
        ,DATA_SOURCE
    FROM BASE
    LEFT JOIN {{ ref('terminology__apr_drg')}} AS TERM ON BASE.APR_DRG_CODE = TERM.APR_DRG_CODE
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
    {{ dbt.listagg(measure="coalesce(cast(Field as varchar), 'null')", delimiter_text="', '", order_by_clause="order by Field desc") }} AS FIELD_AGGREGATED
FROM
    UNIQUE_FIELD
GROUP BY
    CLAIM_ID,
    DATA_SOURCE
	)
SELECT DISTINCT -- to bring to claim_ID grain 
    M.Data_SOURCE
    ,coalesce(cast(M.CLAIM_START_DATE as varchar(50)),cast('1900-01-01' as varchar(10))) AS SOURCE_DATE
    ,'MEDICAL_CLAIM' AS TABLE_NAME
    ,'Claim ID' AS DRILL_DOWN_KEY
    ,coalesce(M.CLAIM_ID, 'NULL') AS DRILL_DOWN_VALUE
    ,'institutional' AS CLAIM_TYPE
    ,'APR_DRG_CODE' AS FIELD_NAME
    ,case when CG.FREQUENCY > 1                then 'multiple'      
          when TERM.APR_DRG_CODE is not null then 'valid'
          when M.APR_DRG_CODE is not null    then 'invalid'      
                                               else 'null' end as BUCKET_NAME
    ,case
        when CG.FREQUENCY = 1
            AND M.APR_DRG_CODE is not null
            AND TERM.APR_DRG_CODE is null
            then 'APR DRG Code does not join to Terminology APR DRG table'
        else null
    end as INVALID_REASON
    ,CAST({{ substring('AGG.FIELD_AGGREGATED', 1, 255) }} AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM BASE M
LEFT JOIN CLAIM_GRAIN CG ON M.CLAIM_ID = CG.CLAIM_ID AND M.Data_Source = CG.Data_Source
LEFT JOIN {{ ref('terminology__apr_drg')}} AS TERM ON M.APR_DRG_CODE = TERM.APR_DRG_CODE
LEFT JOIN CLAIM_AGG AGG ON M.CLAIM_ID = AGG.CLAIM_ID AND M.Data_Source = AGG.Data_Source