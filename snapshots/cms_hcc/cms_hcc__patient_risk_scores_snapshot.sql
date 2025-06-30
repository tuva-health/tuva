{% snapshot cms_hcc__patient_risk_scores_snapshot %}

{% set schema_var %}
{%- if var('tuva_schema_override',None) != None -%}{{var('tuva_schema_override')}}{% else %}{%- if var('tuva_schema_prefix',None) != None -%}{{var('tuva_schema_prefix')}}_cms_hcc{% else %}cms_hcc{%- endif -%}{%- endif -%}
{% endset %}

{% set alias_var %}
{%- if var('tuva_schema_override',None) != None -%}TUVA_CMS_HCC_patient_risk_scores_snapshot{% else %}patient_risk_scores_snapshot{%- endif -%}
{% endset %}

{{
  config({
      "target_schema": schema_var
    , "alias": "patient_risk_scores_snapshot"
    , "tags": "cms_hcc"
    , "strategy": "timestamp"
    , "updated_at": "tuva_last_run"
    , "unique_key": "person_id||payment_year||tuva_last_run"
    , "enabled": var('snapshots_enabled',False) == true and var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) == true | as_bool
  })
}}

select * from {{ ref('cms_hcc__patient_risk_scores') }}

{% endsnapshot %}