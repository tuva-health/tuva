{% snapshot quality_measures__hedis_cql_engine_log_snapshot %}

{% set schema_var %}
{%- if var('tuva_schema_prefix',None) != None -%}{{var('tuva_schema_prefix')}}_quality_measures{% else %}quality_measures{%- endif -%}
{% endset %}

{{
  config({
      "target_schema": schema_var
    , "alias": "hedis_cql_engine_log_snapshot"
    , "tags": ["quality_measures", "hedis"]
    , "strategy": "timestamp"
    , "updated_at": "tuva_last_run"
    , "unique_key": "person_id||measure_id||measure_name||measure_version||cql_concept_key||cql_concept_value||data_source"
    , "enabled": var('snapshots_enabled',False) == true and var('hedis_enabled',False) == true | as_bool
  })
}}

select * from {{ ref('quality_measures__hedis_cql_engine_log') }}

{% endsnapshot %}