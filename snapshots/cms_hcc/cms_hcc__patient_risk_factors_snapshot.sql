{% snapshot cms_hcc__patient_risk_factors_snapshot %}

{% set schema_var %}
{%- if var('tuva_schema_prefix',None) != None -%}{{var('tuva_schema_prefix')}}_cms_hcc{% else %}cms_hcc{%- endif -%}
{% endset %}

{{
  config({
      "target_schema": schema_var
    , "alias": "patient_risk_factors_snapshot"
    , "tags": "cms_hcc"
    , "strategy": "check"
    , "check_cols": ["enrollment_status_default", "medicaid_dual_status_default", "orec_default", "institutional_status_default", "coefficient"]
    , "unique_key": "person_id||payer||factor_type||risk_factor_description||model_version||payment_year"
    , "enabled": var('snapshots_enabled',False) == true and var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) == true | as_bool
    , "hard_deletes": "invalidate"
  })
}}

select * from {{ ref('cms_hcc__patient_risk_factors') }}

{% endsnapshot %}