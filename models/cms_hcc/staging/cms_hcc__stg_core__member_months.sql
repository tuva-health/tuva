{{ config(
     enabled = var('cms_hcc_enabled',var('financial_pmpm_enabled', var('claims_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      person_id
    , year_month
    , payer
    , {{ quote_column('plan') }}
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__member_months') }}
