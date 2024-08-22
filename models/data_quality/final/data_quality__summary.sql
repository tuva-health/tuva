{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}

WITH CTE AS (
    SELECT DISTINCT fm.field_name
    ,fm.input_layer_table_name
    ,fm.claim_type
    ,table_claim_type_field_sk
    FROM {{ ref('data_quality__crosswalk_field_to_mart_sk') }} fm
)

SELECT
    summary_sk,
    fm.table_claim_type_field_sk,
    data_source,
    x.table_name,
    x.claim_type,
    x.field_name,
    sct.red,
    sct.green,
    sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_num,
    sum(case when bucket_name <> 'null' then 1 else 0 end) as fill_num,
    count(drill_down_value) as denom,
    '{{ var('tuva_last_run')}}' as tuva_last_run
FROM
    {{ ref('data_quality__data_quality_detail') }} x
LEFT JOIN CTE fm
    on x.field_name = fm.field_name
    and
    fm.input_layer_table_name = x.table_name
    and
    fm.claim_type = x.claim_type
LEFT JOIN {{ ref('data_quality__crosswalk_field_info') }} sct
    on x.field_name = sct.field_name
    and
    sct.input_layer_table_name = x.table_name
    and
    sct.claim_type = x.claim_type
GROUP BY
    summary_sk,
    data_source,
    fm.table_claim_type_field_sk,
    x.claim_type,
    x.table_name,
    x.field_name,
    sct.red,
    sct.green
