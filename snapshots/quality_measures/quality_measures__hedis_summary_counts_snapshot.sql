{% snapshot quality_measures__hedis_summary_counts_snapshot %}

{% set schema_var %}
{%- if var('tuva_schema_prefix',None) != None -%}{{var('tuva_schema_prefix')}}_quality_measures{% else %}quality_measures{%- endif -%}
{% endset %}

{{
  config({
      "target_schema": schema_var
    , "alias": "hedis_summary_counts_snapshot"
    , "tags": ["quality_measures", "hedis"]
    , "strategy": "timestamp"
    , "updated_at": "tuva_last_run"
    , "unique_key": "measure_id||measure_name||measure_version||performance_period_begin||performance_period_end||rate_1_denominator_sum||rate_1_numerator_sum||rate_1_exclusion_sum||rate_1_performance_rate||rate_1_medicare_denominator_sum||rate_1_medicare_numerator_sum||rate_1_medicare_exclusion_sum||rate_1_medicare_performance_rate||rate_2_denominator_sum||rate_2_numerator_sum||rate_2_exclusion_sum||rate_2_performance_rate||rate_2_medicare_denominator_sum||rate_2_medicare_numerator_sum||rate_2_medicare_exclusion_sum||rate_2_medicare_performance_rate||data_source"
    , "enabled": var('snapshots_enabled',False) == true and var('hedis_enabled',False) == true | as_bool
  })
}}

select * from {{ ref('quality_measures__hedis_summary_counts') }}

{% endsnapshot %}