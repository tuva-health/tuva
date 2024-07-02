{{ config(
     enabled = var('claims_enabled',False)
   )
}}

WITH Ranked_Examples as (
       SELECT
       SUMMARY_SK,
       DATA_SOURCE,
       TABLE_NAME,
       CLAIM_TYPE,
       FIELD_NAME,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       DRILL_DOWN_VALUE as DRILL_DOWN_VALUE, --all claims
       FIELD_VALUE as FIELD_VALUE,
       COUNT(DRILL_DOWN_VALUE) as FREQUENCY,
       ROW_NUMBER() OVER (PARTITION BY SUMMARY_SK, BUCKET_NAME, FIELD_VALUE ORDER BY FIELD_VALUE) AS RN
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_claims_detail') }}
WHERE BUCKET_NAME not in ('valid', 'null')
GROUP BY
       DATA_SOURCE,
       FIELD_NAME,
       TABLE_NAME,
       CLAIM_TYPE,
       BUCKET_NAME,
       FIELD_VALUE,
       DRILL_DOWN_KEY,
       DRILL_DOWN_VALUE,
       INVALID_REASON,
       SUMMARY_SK
       , '{{ var('tuva_last_run')}}'
)
SELECT
       SUMMARY_SK,
       DATA_SOURCE,
       TABLE_NAME,
       CLAIM_TYPE,
       FIELD_NAME,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       MAX(DRILL_DOWN_VALUE) as DRILL_DOWN_VALUE, --1 sample claim
       null as FIELD_VALUE,
       COUNT(DRILL_DOWN_VALUE) as FREQUENCY
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_claims_detail') }}
WHERE BUCKET_NAME = 'null'
GROUP BY
       DATA_SOURCE,
       FIELD_NAME,
       TABLE_NAME,
       CLAIM_TYPE,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       SUMMARY_SK
       , '{{ var('tuva_last_run')}}'
UNION
SELECT
       SUMMARY_SK,
       DATA_SOURCE,
       TABLE_NAME,
       CLAIM_TYPE,
       FIELD_NAME,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       MAX(DRILL_DOWN_VALUE) as DRILL_DOWN_VALUE, --1 sample claim
       FIELD_VALUE as FIELD_VALUE,
       COUNT(DRILL_DOWN_VALUE) as FREQUENCY
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_claims_detail') }}
WHERE BUCKET_NAME = 'valid'
GROUP BY
       DATA_SOURCE,
       FIELD_NAME,
       TABLE_NAME,
       CLAIM_TYPE,
       BUCKET_NAME,
       FIELD_VALUE,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       SUMMARY_SK
       , '{{ var('tuva_last_run')}}'
UNION
SELECT
       SUMMARY_SK,
       DATA_SOURCE,
       TABLE_NAME,
       CLAIM_TYPE,
       FIELD_NAME,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       DRILL_DOWN_VALUE as DRILL_DOWN_VALUE,
       FIELD_VALUE as FIELD_VALUE,
       FREQUENCY
       , '{{ var('tuva_last_run')}}'
FROM Ranked_Examples
WHERE rn <= 5 -- 5 Example claims per unique SK / field value
UNION
SELECT
       SUMMARY_SK,
       DATA_SOURCE,
       TABLE_NAME,
       CLAIM_TYPE,
       FIELD_NAME,
       BUCKET_NAME,
       INVALID_REASON,
       DRILL_DOWN_KEY,
       'All Others' as DRILL_DOWN_VALUE,
       FIELD_VALUE as FIELD_VALUE,
       SUM(FREQUENCY) AS FREQUENCY
       , '{{ var('tuva_last_run')}}'
FROM Ranked_Examples
WHERE rn > 5 -- Aggregating all other rows
GROUP BY
    SUMMARY_SK,
    DATA_SOURCE,
    TABLE_NAME,
    CLAIM_TYPE,
    FIELD_NAME,
    BUCKET_NAME,
    INVALID_REASON,
    DRILL_DOWN_KEY,
    FIELD_VALUE
    , '{{ var('tuva_last_run')}}'