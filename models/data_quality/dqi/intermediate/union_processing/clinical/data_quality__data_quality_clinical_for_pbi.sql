{{ config(
     enabled = var('clinical_enabled',False)
   )
}}

with RANKED_EXAMPLES as (
       select
              SUMMARY_SK
              , DATA_SOURCE
              , TABLE_NAME
              , FIELD_NAME
              , BUCKET_NAME
              , INVALID_REASON
              , DRILL_DOWN_KEY
              , DRILL_DOWN_VALUE as DRILL_DOWN_VALUE
              , FIELD_VALUE as FIELD_VALUE
              , count(DRILL_DOWN_VALUE) as FREQUENCY
              , row_number() over (partition by SUMMARY_SK, BUCKET_NAME, FIELD_VALUE
order by FIELD_VALUE) as RN
              , '{{ var('tuva_last_run') }}' as TUVA_LAST_RUN
       from {{ ref('data_quality__data_quality_clinical_detail') }}
       where BUCKET_NAME not in ('valid', 'null')
       group by
              DATA_SOURCE
              , FIELD_NAME
              , TABLE_NAME
              , BUCKET_NAME
              , FIELD_VALUE
              , DRILL_DOWN_KEY
              , DRILL_DOWN_VALUE
              , INVALID_REASON
              , SUMMARY_SK

)

, PK_EXAMPLES as (
       select
              DETAIL.SUMMARY_SK
              , DETAIL.DATA_SOURCE
              , DETAIL.TABLE_NAME
              , DETAIL.FIELD_NAME
              , DETAIL.BUCKET_NAME
              , DETAIL.INVALID_REASON
              , DETAIL.DRILL_DOWN_KEY
              , DETAIL.DRILL_DOWN_VALUE as DRILL_DOWN_VALUE
              , DETAIL.FIELD_VALUE as FIELD_VALUE
              , count(DETAIL.DRILL_DOWN_VALUE) as FREQUENCY
              , row_number() over (partition by DETAIL.SUMMARY_SK
order by DETAIL.SUMMARY_SK) as RN
              , '{{ var('tuva_last_run') }}' as TUVA_LAST_RUN
       from {{ ref('data_quality__data_quality_clinical_detail') }} as DETAIL
              left outer join {{ ref('data_quality__crosswalk_field_info') }} as FIELD_INFO on DETAIL.TABLE_NAME = FIELD_INFO.INPUT_LAYER_TABLE_NAME
                     and DETAIL.FIELD_NAME = FIELD_INFO.FIELD_NAME
       where DETAIL.BUCKET_NAME = 'valid'
              and FIELD_INFO.UNIQUE_VALUES_EXPECTED_FLAG = 1
       group by
              DETAIL.DATA_SOURCE
              , DETAIL.FIELD_NAME
              , DETAIL.TABLE_NAME
              , DETAIL.BUCKET_NAME
              , DETAIL.FIELD_VALUE
              , DETAIL.DRILL_DOWN_KEY
              , DETAIL.DRILL_DOWN_VALUE
              , DETAIL.INVALID_REASON
              , DETAIL.SUMMARY_SK

)
--- Null Values

select
       SUMMARY_SK
       , DATA_SOURCE
       , TABLE_NAME
       , FIELD_NAME
       , BUCKET_NAME
       , INVALID_REASON
       , DRILL_DOWN_KEY
       , max(DRILL_DOWN_VALUE) as DRILL_DOWN_VALUE
       , null as FIELD_VALUE
       , count(DRILL_DOWN_VALUE) as FREQUENCY
       , '{{ var('tuva_last_run') }}' as TUVA_LAST_RUN
from {{ ref('data_quality__data_quality_clinical_detail') }}
where BUCKET_NAME = 'null'
group by
       DATA_SOURCE
       , FIELD_NAME
       , TABLE_NAME
       , BUCKET_NAME
       , INVALID_REASON
       , DRILL_DOWN_KEY
       , SUMMARY_SK

union all

--- Valid Values except PKs

select
       DETAIL.SUMMARY_SK
       , DETAIL.DATA_SOURCE
       , DETAIL.TABLE_NAME
       , DETAIL.FIELD_NAME
       , DETAIL.BUCKET_NAME
       , DETAIL.INVALID_REASON
       , DETAIL.DRILL_DOWN_KEY
       , max(DETAIL.DRILL_DOWN_VALUE) as DRILL_DOWN_VALUE
       , DETAIL.FIELD_VALUE as FIELD_VALUE
       , count(DETAIL.DRILL_DOWN_VALUE) as FREQUENCY
       , '{{ var('tuva_last_run') }}' as TUVA_LAST_RUN
from {{ ref('data_quality__data_quality_clinical_detail') }} as DETAIL
left outer join {{ ref('data_quality__crosswalk_field_info') }} as FIELD_INFO on DETAIL.TABLE_NAME = FIELD_INFO.INPUT_LAYER_TABLE_NAME
       and DETAIL.FIELD_NAME = FIELD_INFO.FIELD_NAME
where
       DETAIL.BUCKET_NAME = 'valid'
       and FIELD_INFO.UNIQUE_VALUES_EXPECTED_FLAG = 0 --- need to handle pks differently since every value is supposed to be unique
group by
       DETAIL.DATA_SOURCE
       , DETAIL.FIELD_NAME
       , DETAIL.TABLE_NAME
       , DETAIL.BUCKET_NAME
       , DETAIL.FIELD_VALUE
       , DETAIL.INVALID_REASON
       , DETAIL.DRILL_DOWN_KEY
       , DETAIL.SUMMARY_SK

union all

-- 5 Examples of each invalid example

select
       SUMMARY_SK
       , DATA_SOURCE
       , TABLE_NAME
       , FIELD_NAME
       , BUCKET_NAME
       , INVALID_REASON
       , DRILL_DOWN_KEY
       , DRILL_DOWN_VALUE as DRILL_DOWN_VALUE
       , FIELD_VALUE as FIELD_VALUE
       , FREQUENCY
       , '{{ var('tuva_last_run') }}' as TUVA_LAST_RUN
from RANKED_EXAMPLES
where RN <= 5

union all

--- Aggregating all other invalid examples into single row

select
       SUMMARY_SK
       , DATA_SOURCE
       , TABLE_NAME
       , FIELD_NAME
       , BUCKET_NAME
       , INVALID_REASON
       , DRILL_DOWN_KEY
       , 'all others' as DRILL_DOWN_VALUE
       , FIELD_VALUE as FIELD_VALUE
       , sum(FREQUENCY) as FREQUENCY
       , '{{ var('tuva_last_run') }}' as TUVA_LAST_RUN
from RANKED_EXAMPLES
where RN > 5 --- Aggregating all other rows
group by
    SUMMARY_SK
    , DATA_SOURCE
    , TABLE_NAME
    , FIELD_NAME
    , BUCKET_NAME
    , INVALID_REASON
    , DRILL_DOWN_KEY
    , FIELD_VALUE

union all

--- 5 Examples of valid primary key values

select
       SUMMARY_SK
       , DATA_SOURCE
       , TABLE_NAME
       , FIELD_NAME
       , BUCKET_NAME
       , INVALID_REASON
       , DRILL_DOWN_KEY
       , DRILL_DOWN_VALUE as DRILL_DOWN_VALUE
       , FIELD_VALUE as FIELD_VALUE
       , FREQUENCY
       , '{{ var('tuva_last_run') }}' as TUVA_LAST_RUN
from PK_EXAMPLES
where RN <= 5

union all

--- Aggegating all other valid primary key value examples

select
       SUMMARY_SK
       , DATA_SOURCE
       , TABLE_NAME
       , FIELD_NAME
       , BUCKET_NAME
       , INVALID_REASON
       , DRILL_DOWN_KEY
       , 'All Others' as DRILL_DOWN_VALUE
       , 'All Others' as FIELD_VALUE
       , sum(FREQUENCY) as FREQUENCY
       , '{{ var('tuva_last_run') }}' as TUVA_LAST_RUN
from PK_EXAMPLES
where RN > 5 --- Aggregating all other rows
group by
    SUMMARY_SK
    , DATA_SOURCE
    , TABLE_NAME
    , FIELD_NAME
    , BUCKET_NAME
    , INVALID_REASON
    , DRILL_DOWN_KEY
    , FIELD_VALUE
