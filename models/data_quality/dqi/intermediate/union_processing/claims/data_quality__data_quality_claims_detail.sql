{{ config(
     enabled = var('claims_enabled',False)
   )
}}

select
    data_source
  , source_date
  , table_name
  , drill_down_key
  , drill_down_value
  , claim_type
  , field_name
  , bucket_name
  , invalid_reason
  , field_value
  , tuva_last_run
  , dense_rank() over (
        order by data_source
               , table_name
               , claim_type
               , field_name
    ) as summary_sk
from {{ ref('data_quality__data_quality_claims_detail_union') }}
