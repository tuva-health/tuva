
{% if var('attribution_claims_source') == 'cclf' %}

  select *
  from {{ref('cms_provider_attribution__stg_cclf1')}}

{% elif var('attribution_claims_source') == 'bcda' %}

  select *
  from {{ref('cms_provider_attribution__stg_cclf1_bcda')}}

{% endif %}