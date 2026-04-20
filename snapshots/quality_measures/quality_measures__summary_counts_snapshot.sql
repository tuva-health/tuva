{% snapshot quality_measures__summary_counts_snapshot %}

{% set schema_var %}
{%- if var('tuva_schema_prefix',None) != None -%}{{var('tuva_schema_prefix')}}_quality_measures{% else %}quality_measures{%- endif -%}
{% endset %}

{{
  config({
      "target_schema": schema_var
    , "alias": "summary_counts_snapshot"
    , "tags": "quality_measures"
    , "strategy": "check"
    , "check_cols": ["denominator_sum", "numerator_sum", "exclusion_sum", "performance_rate"]
    , "unique_key": "measure_id||measure_name||measure_version||performance_period_begin||performance_period_end"
    , "enabled": var('snapshots_enabled',False) == true and var('claims_enabled', var('clinical_enabled', False)) == true | as_bool
    , "hard_deletes": "invalidate"
  })
}}

select * from {{ ref('quality_measures__summary_counts') }}

{% endsnapshot %}
