{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}

WITH CTE AS (
    SELECT DISTINCT FM.FIELD_NAME
    ,FM.INPUT_LAYER_TABLE_NAME
    ,FM.CLAIM_TYPE
    ,TABLE_CLAIM_TYPE_FIELD_SK
    FROM {{ ref('data_quality__crosswalk_field_to_mart_sk') }} FM
)

SELECT 
    SUMMARY_SK,
    FM.TABLE_CLAIM_TYPE_FIELD_SK,
    DATA_SOURCE,
    X.TABLE_NAME,
    X.CLAIM_TYPE,
    X.FIELD_NAME,
    SCT.RED,
    SCT.GREEN,
    SUM(CASE WHEN BUCKET_NAME = 'valid' THEN 1 ELSE 0 END) as VALID_NUM,
    SUM(CASE WHEN BUCKET_NAME <> 'null' THEN 1 ELSE 0 END) as FILL_NUM,
    COUNT(DRILL_DOWN_VALUE) as DENOM,
    '{{ var('tuva_last_run')}}' as tuva_last_run
FROM
    {{ ref('data_quality__data_quality_detail') }} X
LEFT JOIN CTE FM 
    ON X.FIELD_NAME = FM.FIELD_NAME
    AND
    FM.INPUT_LAYER_TABLE_NAME = X.TABLE_NAME
    AND
    FM.CLAIM_TYPE = X.CLAIM_TYPE
LEFT JOIN {{ ref('data_quality__crosswalk_field_info') }} SCT
    ON X.FIELD_NAME = SCT.FIELD_NAME
    AND
    SCT.INPUT_LAYER_TABLE_NAME = X.TABLE_NAME
    AND
    SCT.CLAIM_TYPE = x.CLAIM_TYPE
GROUP BY 
    SUMMARY_SK,
    DATA_SOURCE,
    FM.TABLE_CLAIM_TYPE_FIELD_SK,
    X.CLAIM_TYPE,
    X.TABLE_NAME,
    X.FIELD_NAME,
    SCT.RED,
    SCT.GREEN