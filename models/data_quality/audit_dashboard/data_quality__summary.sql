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
, final as (
        SELECT
        summary_sk,
        fm.table_claim_type_field_sk,
        data_source,
        lower(x.table_name) as table_name,
        x.claim_type,
        x.field_name,
        sct.red,
        sct.green,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_num,
        sum(case when bucket_name <> 'null' then 1 else 0 end) as fill_num,
        count(drill_down_value)                                as denom,
        '{{ var('tuva_last_run')}}'                            as tuva_last_run
    FROM {{ ref('data_quality__data_quality_detail') }} x
    LEFT JOIN CTE fm
        on x.field_name = fm.field_name
    and fm.input_layer_table_name = x.table_name
    and fm.claim_type = x.claim_type
    LEFT JOIN {{ ref('data_quality__crosswalk_field_info') }} sct
        on x.field_name = sct.field_name
    and sct.input_layer_table_name = x.table_name
    and sct.claim_type = x.claim_type
    GROUP BY
        summary_sk,
        data_source,
        fm.table_claim_type_field_sk,
        x.claim_type,
        x.table_name,
        x.field_name,
        sct.red,
        sct.green
)

select
    summary_sk
    , table_claim_type_field_sk
    , data_source
    , table_name
    , claim_type
    , field_name
    , cast(red as int) as red
    , cast(green as int) as green
    , denom
    , fill_num
    , case
        when fill_num <> 0 then (fill_num/denom) * 100
            else '0'
    end as percent_fill
    , case
        when percent_fill = 0 then 'field empty'
        when percent_fill <= red then 'out of bounds'
            else 'pass'
     end as fill_status
    , valid_num
    , case
        when valid_num <> 0 then (valid_num/denom) * 100
            else '0'
    end as percent_valid
    ,case
        when (percent_fill - percent_valid) >= 20 then 'valid values low compared to fill rate'
        when percent_valid <= red then 'out of bounds'
            else 'pass'
     end as valid_status
    , tuva_last_run
from final





