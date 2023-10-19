{% snapshot quality_measures__summary_long_snapshot %}

{% set schema_var %}
{%- if var('tuva_schema_prefix',None) != None -%}{{var('tuva_schema_prefix')}}_quality_measures{% else %}quality_measures{%- endif -%}
{% endset %}

{{
  config({
      "target_schema": schema_var
    , "alias": "summary_long_snapshot"
    , "tags": "quality_measures"
    , "strategy": "timestamp"
    , "updated_at": "tuva_last_run"
    , "unique_key": "patient_id||denominator_flag||numerator_flag||exclusion_flag||evidence_date||exclusion_date||exclusion_reason||performance_period_begin||performance_period_end||measure_id||measure_name||measure_version||tuva_last_run"
    , "enabled": var('snapshots_enabled',False) == true and var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) == true | as_bool
  })
}}

select * from {{ ref('quality_measures__summary_long') }}

{% endsnapshot %}