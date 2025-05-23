{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

with final_before_attribution_fields as (
  select distinct
      a.member_month_key
    , a.person_id
    , a.member_id
    , a.year_month
    , a.payer
    , a.{{ quote_column('plan') }}
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
  from {{ ref('normalized_input__eligibility') }} as a
)

, add_attribution_fields as (
  select
      a.member_month_key
    , a.person_id
    , a.member_id
    , a.year_month
    , a.payer
    , a.{{ quote_column('plan') }}
    , a.data_source
    , a.tuva_last_run

    , b.payer_attributed_provider
    , b.payer_attributed_provider_practice
    , b.payer_attributed_provider_organization
    , b.payer_attributed_provider_lob
    , b.custom_attributed_provider
    , b.custom_attributed_provider_practice
    , b.custom_attributed_provider_organization
    , b.custom_attributed_provider_lob

  from final_before_attribution_fields as a
  left outer join {{ ref('financial_pmpm__stg_provider_attribution') }} as b
  on a.person_id = b.person_id
  and a.year_month = b.year_month
  and a.payer = b.payer
  and a.{{ quote_column('plan') }} = b.{{ quote_column('plan') }}
  and a.data_source = b.data_source
)

select
    member_month_key
  , person_id
  , member_id
  , year_month
  , payer
  , {{ quote_column('plan') }}
  , data_source
  , tuva_last_run
  , payer_attributed_provider
  , payer_attributed_provider_practice
  , payer_attributed_provider_organization
  , payer_attributed_provider_lob
  , custom_attributed_provider
  , custom_attributed_provider_practice
  , custom_attributed_provider_organization
  , custom_attributed_provider_lob
from add_attribution_fields
