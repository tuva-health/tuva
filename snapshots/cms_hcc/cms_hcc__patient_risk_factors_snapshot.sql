{% snapshot cms_hcc__patient_risk_factors_snapshot %}

{% set schema_var %}
{%- if var('tuva_schema_prefix',None) != None -%}{{var('tuva_schema_prefix')}}_cms_hcc{% else %}cms_hcc{%- endif -%}
{% endset %}

{{
  config({
      "target_schema": schema_var
    , "alias": "patient_risk_factors_snapshot"
    , "tags": "cms_hcc"
    , "strategy": "timestamp"
    , "updated_at": "tuva_last_run"
    , "unique_key": "patient_id||model_version||payment_year||tuva_last_run"
    , "enabled": var('snapshots_enabled',False) == true and var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) == true | as_bool
  })
}}

select * from {{ ref('cms_hcc__patient_risk_factors') }}

{% endsnapshot %}