{{ config(
     enabled = var('claims_enabled',False)
   )
}}

WITH ranked_examples as (
       select
       summary_sk,
       data_source,
       table_name,
       claim_type,
       field_name,
       bucket_name,
       invalid_reason,
       drill_down_key,
       drill_down_value as drill_down_value, --all claims
       field_value as field_value,
       count(drill_down_value) as frequency,
       row_number() over (partition by summary_sk, bucket_name, field_value order by field_value) as rn
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_claims_detail') }}
WHERE bucket_name not in ('valid', 'null')
GROUP BY
       data_source,
       field_name,
       table_name,
       claim_type,
       bucket_name,
       field_value,
       drill_down_key,
       drill_down_value,
       invalid_reason,
       summary_sk
)
SELECT
       summary_sk,
       data_source,
       table_name,
       claim_type,
       field_name,
       bucket_name,
       invalid_reason,
       drill_down_key,
       max(drill_down_value) as drill_down_value, --1 sample claim
       null as field_value,
       count(drill_down_value) as frequency
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_claims_detail') }}
WHERE bucket_name = 'null'
GROUP BY
       data_source,
       field_name,
       table_name,
       claim_type,
       bucket_name,
       invalid_reason,
       drill_down_key,
       summary_sk

union all
SELECT
       summary_sk,
       data_source,
       table_name,
       claim_type,
       field_name,
       bucket_name,
       invalid_reason,
       drill_down_key,
       max(drill_down_value) as drill_down_value, --1 sample claim
       field_value as field_value,
       count(drill_down_value) as frequency
       , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('data_quality__data_quality_claims_detail') }}
WHERE bucket_name = 'valid'
GROUP BY
       data_source,
       field_name,
       table_name,
       claim_type,
       bucket_name,
       field_value,
       invalid_reason,
       drill_down_key,
       summary_sk

union all
SELECT
       summary_sk,
       data_source,
       table_name,
       claim_type,
       field_name,
       bucket_name,
       invalid_reason,
       drill_down_key,
       drill_down_value as drill_down_value,
       field_value as field_value,
       frequency
       , '{{ var('tuva_last_run')}}'
FROM ranked_examples
WHERE rn <= 5 -- 5 Example claims per unique SK / field value
union all
SELECT
       summary_sk,
       data_source,
       table_name,
       claim_type,
       field_name,
       bucket_name,
       invalid_reason,
       drill_down_key,
       'All Others' as drill_down_value,
       field_value as field_value,
       sum(frequency) as frequency
       , '{{ var('tuva_last_run')}}'
FROM ranked_examples
WHERE rn > 5 -- Aggregating all other rows
GROUP BY
    summary_sk,
    data_source,
    table_name,
    claim_type,
    field_name,
    bucket_name,
    invalid_reason,
    drill_down_key,
    field_value
