{{ config(
     enabled = var('cms_hcc_enabled',var('financial_pmpm_enabled', var('claims_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
     patient_id
    , year_month
    , payer
    {% if target.type == 'fabric' %}
        , "plan"
    {% else %}
        , plan
    {% endif %}
    , data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__member_months') }}