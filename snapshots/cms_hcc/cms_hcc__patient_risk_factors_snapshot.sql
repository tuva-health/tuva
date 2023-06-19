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
    , "updated_at": "last_update"
    , "unique_key": "patient_id||model_version||payment_year||last_update"
    , "enabled": var('cms_hcc_enabled',var('tuva_marts_enabled',True))
  })
}}

select * from {{ ref('cms_hcc__patient_risk_factors') }}

{% endsnapshot %}