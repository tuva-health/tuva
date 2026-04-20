{% snapshot quality_measures__summary_long_snapshot %}

{% set schema_var %}
{%- if var('tuva_schema_prefix',None) != None -%}{{var('tuva_schema_prefix')}}_quality_measures{% else %}quality_measures{%- endif -%}
{% endset %}

{{
  config({
      "target_schema": schema_var
    , "alias": "summary_long_snapshot"
    , "tags": "quality_measures"
    , "strategy": "check"
    , "check_cols": ["denominator_flag", "numerator_flag", "exclusion_flag", "performance_flag", "evidence_date", "evidence_value", "exclusion_date", "exclusion_reason"]
    , "unique_key": "person_id||coalesce(measure_id, '')||coalesce(measure_name, '')||coalesce(measure_version, '')||coalesce(cast(performance_period_begin as varchar), '')||coalesce(cast(performance_period_end as varchar), '')"
    , "enabled": var('snapshots_enabled',False) == true and var('claims_enabled', var('clinical_enabled', False)) == true | as_bool
    , "hard_deletes": "invalidate"
  })
}}

select * from {{ ref('quality_measures__summary_long') }}

{% endsnapshot %}
