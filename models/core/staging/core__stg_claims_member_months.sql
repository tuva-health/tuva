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
  from {{ ref('claims_enrollment__member_months') }} as a
)

select
    a.member_month_key
  , a.person_id
  , a.member_id
  , a.year_month
  , a.payer
  , a.{{ quote_column('plan') }}
  , a.data_source
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
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
and a.member_id = b.member_id
and a.year_month = b.year_month
and a.payer = b.payer
and a.{{ quote_column('plan') }} = b.{{ quote_column('plan') }}
and a.data_source = b.data_source
