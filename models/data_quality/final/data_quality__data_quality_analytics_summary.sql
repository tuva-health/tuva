{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

with unioned_data as (

  -- Unioning multiple data quality checks, excluding the _loaded_at field
  {{ dbt_utils.union_relations(
      relations=[
          ref('data_quality__readmissions')
      ],
      exclude=["_loaded_at"]
  ) }}

)

select
  data_quality_check
  , result_count
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from unioned_data

