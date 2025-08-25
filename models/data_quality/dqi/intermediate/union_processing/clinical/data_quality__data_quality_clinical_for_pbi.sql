{{ config(
     enabled = var('clinical_enabled',false)
   )
}}

with ranked_examples as (
       select
              summary_sk
              , data_source
              , table_name
              , field_name
              , bucket_name
              , invalid_reason
              , drill_down_key
              , drill_down_value as drill_down_value
              , field_value as field_value
              , count(drill_down_value) as frequency
              , row_number() over (partition by summary_sk, bucket_name, field_value
order by field_value) as rn
              , '{{ var('tuva_last_run') }}' as tuva_last_run
       from {{ ref('data_quality__data_quality_clinical_detail') }}
       where bucket_name not in ('valid', 'null')
       group by
              data_source
              , field_name
              , table_name
              , bucket_name
              , field_value
              , drill_down_key
              , drill_down_value
              , invalid_reason
              , summary_sk

)

, pk_examples as (
       select
              detail.summary_sk
              , detail.data_source
              , detail.table_name
              , detail.field_name
              , detail.bucket_name
              , detail.invalid_reason
              , detail.drill_down_key
              , detail.drill_down_value as drill_down_value
              , detail.field_value as field_value
              , count(detail.drill_down_value) as frequency
              , row_number() over (partition by detail.summary_sk
order by detail.summary_sk) as rn
              , '{{ var('tuva_last_run') }}' as tuva_last_run
       from {{ ref('data_quality__data_quality_clinical_detail') }} as detail
              left outer join {{ ref('data_quality__crosswalk_field_info') }} as field_info on detail.table_name = field_info.input_layer_table_name
                     and detail.field_name = field_info.field_name
       where detail.bucket_name = 'valid'
              and field_info.unique_values_expected_flag = 1
       group by
              detail.data_source
              , detail.field_name
              , detail.table_name
              , detail.bucket_name
              , detail.field_value
              , detail.drill_down_key
              , detail.drill_down_value
              , detail.invalid_reason
              , detail.summary_sk

)
--- null values

select
       summary_sk
       , data_source
       , table_name
       , field_name
       , bucket_name
       , invalid_reason
       , drill_down_key
       , max(drill_down_value) as drill_down_value
       , null as field_value
       , count(drill_down_value) as frequency
       , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__data_quality_clinical_detail') }}
where bucket_name = 'null'
group by
       data_source
       , field_name
       , table_name
       , bucket_name
       , invalid_reason
       , drill_down_key
       , summary_sk

union all

--- valid values except pks

select
       detail.summary_sk
       , detail.data_source
       , detail.table_name
       , detail.field_name
       , detail.bucket_name
       , detail.invalid_reason
       , detail.drill_down_key
       , max(detail.drill_down_value) as drill_down_value
       , detail.field_value as field_value
       , count(detail.drill_down_value) as frequency
       , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__data_quality_clinical_detail') }} as detail
left outer join {{ ref('data_quality__crosswalk_field_info') }} as field_info on detail.table_name = field_info.input_layer_table_name
       and detail.field_name = field_info.field_name
where
       detail.bucket_name = 'valid'
       and field_info.unique_values_expected_flag = 0 --- need to handle pks differently since every value is supposed to be unique
group by
       detail.data_source
       , detail.field_name
       , detail.table_name
       , detail.bucket_name
       , detail.field_value
       , detail.invalid_reason
       , detail.drill_down_key
       , detail.summary_sk

union all

-- 5 examples of each invalid example

select
       summary_sk
       , data_source
       , table_name
       , field_name
       , bucket_name
       , invalid_reason
       , drill_down_key
       , drill_down_value as drill_down_value
       , field_value as field_value
       , frequency
       , '{{ var('tuva_last_run') }}' as tuva_last_run
from ranked_examples
where rn <= 5

union all

--- aggregating all other invalid examples into single row

select
       summary_sk
       , data_source
       , table_name
       , field_name
       , bucket_name
       , invalid_reason
       , drill_down_key
       , 'all others' as drill_down_value
       , field_value as field_value
       , sum(frequency) as frequency
       , '{{ var('tuva_last_run') }}' as tuva_last_run
from ranked_examples
where rn > 5 --- aggregating all other rows
group by
    summary_sk
    , data_source
    , table_name
    , field_name
    , bucket_name
    , invalid_reason
    , drill_down_key
    , field_value

union all

--- 5 examples of valid primary key values

select
       summary_sk
       , data_source
       , table_name
       , field_name
       , bucket_name
       , invalid_reason
       , drill_down_key
       , drill_down_value as drill_down_value
       , field_value as field_value
       , frequency
       , '{{ var('tuva_last_run') }}' as tuva_last_run
from pk_examples
where rn <= 5

union all

--- aggegating all other valid primary key value examples

select
       summary_sk
       , data_source
       , table_name
       , field_name
       , bucket_name
       , invalid_reason
       , drill_down_key
       , 'all others' as drill_down_value
       , 'all others' as field_value
       , sum(frequency) as frequency
       , '{{ var('tuva_last_run') }}' as tuva_last_run
from pk_examples
where rn > 5 --- aggregating all other rows
group by
    summary_sk
    , data_source
    , table_name
    , field_name
    , bucket_name
    , invalid_reason
    , drill_down_key
    , field_value
