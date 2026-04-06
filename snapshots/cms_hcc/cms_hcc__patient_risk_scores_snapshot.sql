{% snapshot cms_hcc__patient_risk_scores_snapshot %}

{% set schema_var %}
{%- if var('tuva_schema_prefix',None) != None -%}{{var('tuva_schema_prefix')}}_cms_hcc{% else %}cms_hcc{%- endif -%}
{% endset %}

{{
  config({
      "target_schema": schema_var
    , "alias": "patient_risk_scores_snapshot"
    , "tags": "cms_hcc"
    , "strategy": "check"
    , "check_cols": ["v24_risk_score", "v28_risk_score", "blended_risk_score", "normalized_risk_score", "payment_risk_score", "payment_risk_score_weighted_by_months", "member_months"]
    , "unique_key": "person_id||payer||payment_year"
    , "enabled": var('snapshots_enabled',False) == true and var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) == true | as_bool
    , "hard_deletes": "invalidate"
  })
}}

select * from {{ ref('cms_hcc__patient_risk_scores') }}

{% endsnapshot %}