{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

with unioned_data as (

  -- Unioning multiple data quality checks, excluding the _loaded_at field
  {{ dbt_utils.union_relations(
      relations=[
          ref('data_quality__readmissions_reference')
        , ref('data_quality__cms_hcc_reference')
      ],
      exclude=["_loaded_at"]
  ) }}

)

select
  analytics_measure
  , data_source_value
  , analytics_value
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from unioned_data

