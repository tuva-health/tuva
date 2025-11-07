{{ config(
     enabled = var('cms_hcc_enabled',var('financial_pmpm_enabled', var('claims_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
-- Need distinct because of plan in core__member_months
select distinct
      person_id
    , payer
    , year_month
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__member_months') }}
