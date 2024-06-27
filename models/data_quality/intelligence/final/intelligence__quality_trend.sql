{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}

select
    source_date
    ,summary_sk
    ,SUM(CASE WHEN BUCKET_NAME = 'valid' THEN 1 ELSE 0 END) as VALID_NUM
    ,SUM(CASE WHEN BUCKET_NAME <> 'null' THEN 1 ELSE 0 END) as FILL_NUM
    ,COUNT(DRILL_DOWN_VALUE) as DENOM
FROM {{ ref('intelligence__data_quality_detail') }}
group by
    source_date
    ,summary_sk