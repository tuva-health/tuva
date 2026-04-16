{{ config(
     enabled = (var('enable_legacy_data_quality', false) | as_bool)
       and (var('clinical_enabled', false) | as_bool)
   )
}}

select
      data_source
    , source_date
    , table_name
    , drill_down_key
    , drill_down_value
    , field_name
    , bucket_name
    , invalid_reason
    , field_value
    , tuva_last_run
    , dense_rank() over (
order by data_source, table_name, field_name) + 100000 as summary_sk
from {{ ref("data_quality__data_quality_clinical_detail_union") }}
