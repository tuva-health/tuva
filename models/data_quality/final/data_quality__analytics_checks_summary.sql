{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with unioned_data as (

  -- Unioning multiple data quality checks, excluding the _loaded_at field
  {{ dbt_utils.union_relations(
      relations=[
          ref('data_quality__readmissions')
        , ref('data_quality__chronic_conditions_none')
        , ref('data_quality__cms_hcc')
        , ref('data_quality__quality_measures')
        , ref('data_quality__encounters_missing_groups_union')
        , ref('data_quality__acute_inpatient')
        , ref('data_quality__ed_classification')
      ],
      exclude=["_loaded_at"]
  ) }}

)

select
  data_quality_check
  , coalesce(result_count,0) as result_count
  , coalesce(normally_zero ,1) as normally_zero
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from unioned_data