{{ config(
     enabled = var('clinical_enabled',False)
   )
}}

WITH Ranked_Examples as (
       SELECT
              SUMMARY_SK,
              DATA_SOURCE,
              TABLE_NAME,
              FIELD_NAME,
              BUCKET_NAME,
              INVALID_REASON,
              DRILL_DOWN_KEY,
              DRILL_DOWN_VALUE as DRILL_DOWN_VALUE,
              FIELD_VALUE as FIELD_VALUE,
              COUNT(DRILL_DOWN_VALUE) as FREQUENCY,
              ROW_NUMBER() OVER (PARTITION BY SUMMARY_SK, BUCKET_NAME, FIELD_VALUE ORDER BY FIELD_VALUE) AS RN
              , '{{ var('tuva_last_run')}}' as tuva_last_run
       FROM {{ ref('data_quality__data_quality_clinical_detail') }}
       WHERE BUCKET_NAME not in ('valid', 'null')
       GROUP BY
              DATA_SOURCE,
              FIELD_NAME,
              TABLE_NAME,
              BUCKET_NAME,
              FIELD_VALUE,
              DRILL_DOWN_KEY,
              DRILL_DOWN_VALUE,
              INVALID_REASON,
              SUMMARY_SK

),

pk_examples as (
       SELECT
              detail.SUMMARY_SK,
              detail.DATA_SOURCE,
              detail.TABLE_NAME,
              detail.FIELD_NAME,
              detail.BUCKET_NAME,
              detail.INVALID_REASON,
              detail.DRILL_DOWN_KEY,
              detail.DRILL_DOWN_VALUE as DRILL_DOWN_VALUE,
              detail.FIELD_VALUE as FIELD_VALUE,
              COUNT(detail.DRILL_DOWN_VALUE) as FREQUENCY,
              ROW_NUMBER() OVER (PARTITION BY detail.SUMMARY_SK ORDER BY detail.SUMMARY_SK) AS RN
              , '{{ var('tuva_last_run')}}' as tuva_last_run
       FROM {{ ref('data_quality__data_quality_clinical_detail') }} as detail
              left join {{ ref('data_quality__crosswalk_field_info')}} as field_info on detail.table_name = field_info.INPUT_LAYER_TABLE_NAME
                     and detail.field_name = field_info.field_name
       WHERE detail.BUCKET_NAME = 'valid'
              AND field_info.UNIQUE_VALUES_EXPECTED_FLAG = 1
       GROUP BY
              detail.DATA_SOURCE,
              detail.FIELD_NAME,
              detail.TABLE_NAME,
              detail.BUCKET_NAME,
              detail.FIELD_VALUE,
              detail.DRILL_DOWN_KEY,
              detail.DRILL_DOWN_VALUE,
              detail.INVALID_REASON,
              detail.SUMMARY_SK

)
--- Null Values

SELECT
       SUMMARY_SK,
       DATA_SOURCE,
       TABLE_NAME,
       FIELD_NAME,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       MAX(DRILL_DOWN_VALUE) as DRILL_DOWN_VALUE,
       null as FIELD_VALUE,
       COUNT(DRILL_DOWN_VALUE) as FREQUENCY
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_clinical_detail') }}
WHERE BUCKET_NAME = 'null'
GROUP BY
       DATA_SOURCE,
       FIELD_NAME,
       TABLE_NAME,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       SUMMARY_SK

union all

--- Valid Values except PKs

SELECT
       detail.SUMMARY_SK,
       detail.DATA_SOURCE,
       detail.TABLE_NAME,
       detail.FIELD_NAME,
       detail.BUCKET_NAME,
       detail.INVALID_REASON,
       detail.DRILL_DOWN_KEY,
       MAX(detail.DRILL_DOWN_VALUE) as DRILL_DOWN_VALUE,
       detail.FIELD_VALUE as FIELD_VALUE,
       COUNT(detail.DRILL_DOWN_VALUE) as FREQUENCY
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_clinical_detail') }} as detail
LEFT JOIN {{ ref('data_quality__crosswalk_field_info') }} as field_info ON detail.table_name = field_info.INPUT_LAYER_TABLE_NAME
       and detail.field_name = field_info.field_name
WHERE 
       detail.BUCKET_NAME = 'valid'
       AND field_info.UNIQUE_VALUES_EXPECTED_FLAG = 0 --- Need to handle PKs differently since every value is supposed to be unique
GROUP BY
       detail.DATA_SOURCE,
       detail.FIELD_NAME,
       detail.TABLE_NAME,
       detail.BUCKET_NAME,
       detail.FIELD_VALUE,
       detail.INVALID_REASON,
       detail.DRILL_DOWN_KEY,
       detail.SUMMARY_SK

union all

-- 5 Examples of each invalid example

SELECT
       SUMMARY_SK,
       DATA_SOURCE,
       TABLE_NAME,
       FIELD_NAME,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       DRILL_DOWN_VALUE as DRILL_DOWN_VALUE,
       FIELD_VALUE as FIELD_VALUE,
       FREQUENCY
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM Ranked_Examples
WHERE rn <= 5

union all

--- Aggregating all other invalid examples into single row

SELECT
       SUMMARY_SK,
       DATA_SOURCE,
       TABLE_NAME,
       FIELD_NAME,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       'All Others' as DRILL_DOWN_VALUE,
       FIELD_VALUE as FIELD_VALUE,
       SUM(FREQUENCY) AS FREQUENCY
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM Ranked_Examples
WHERE rn > 5 --- Aggregating all other rows
GROUP BY
    SUMMARY_SK,
    DATA_SOURCE,
    TABLE_NAME,
    FIELD_NAME,
    BUCKET_NAME,
    INVALID_REASON,
    DRILL_DOWN_KEY,
    FIELD_VALUE

union all

--- 5 Examples of valid primary key values

SELECT
       SUMMARY_SK,
       DATA_SOURCE,
       TABLE_NAME,
       FIELD_NAME,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       DRILL_DOWN_VALUE as DRILL_DOWN_VALUE,
       FIELD_VALUE as FIELD_VALUE,
       FREQUENCY
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM pk_examples
WHERE rn <= 5

union all

--- Aggegating all other valid primary key value examples

SELECT
       SUMMARY_SK,
       DATA_SOURCE,
       TABLE_NAME,
       FIELD_NAME,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       'All Others' as DRILL_DOWN_VALUE,
       'All Others' as FIELD_VALUE,
       SUM(FREQUENCY) AS FREQUENCY
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM pk_examples
WHERE rn > 5 --- Aggregating all other rows
GROUP BY
    SUMMARY_SK,
    DATA_SOURCE,
    TABLE_NAME,
    FIELD_NAME,
    BUCKET_NAME,
    INVALID_REASON,
    DRILL_DOWN_KEY,
    FIELD_VALUE
