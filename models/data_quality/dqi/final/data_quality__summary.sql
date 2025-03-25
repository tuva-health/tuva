{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}

with CTE as (
    select distinct FM.FIELD_NAME
    , FM.INPUT_LAYER_TABLE_NAME
    , FM.CLAIM_TYPE
    , TABLE_CLAIM_TYPE_FIELD_SK
    from {{ ref('data_quality__crosswalk_field_to_mart_sk') }} as FM
)

select
    SUMMARY_SK
    , FM.TABLE_CLAIM_TYPE_FIELD_SK
    , DATA_SOURCE
    , X.TABLE_NAME
    , X.CLAIM_TYPE
    , X.FIELD_NAME
    , SCT.RED
    , SCT.GREEN
    , sum(case when BUCKET_NAME = 'valid' then 1 else 0 end) as VALID_NUM
    , sum(case when BUCKET_NAME <> 'null' then 1 else 0 end) as FILL_NUM
    , count(DRILL_DOWN_VALUE) as DENOM
    , '{{ var('tuva_last_run') }}' as TUVA_LAST_RUN
from
    {{ ref('data_quality__data_quality_detail') }} as X
left outer join CTE as FM
    on X.FIELD_NAME = FM.FIELD_NAME
    and
    FM.INPUT_LAYER_TABLE_NAME = X.TABLE_NAME
    and
    FM.CLAIM_TYPE = X.CLAIM_TYPE
left outer join {{ ref('data_quality__crosswalk_field_info') }} as SCT
    on X.FIELD_NAME = SCT.FIELD_NAME
    and
    SCT.INPUT_LAYER_TABLE_NAME = X.TABLE_NAME
    and
    SCT.CLAIM_TYPE = X.CLAIM_TYPE
group by
    SUMMARY_SK
    , DATA_SOURCE
    , FM.TABLE_CLAIM_TYPE_FIELD_SK
    , X.CLAIM_TYPE
    , X.TABLE_NAME
    , X.FIELD_NAME
    , SCT.RED
    , SCT.GREEN
