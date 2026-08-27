{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select
      mm.member_month_id
    , mm.person_id
    , mm.member_id
    , mm.year_month
    , mm.payer
    , mm.{{ quote_column('plan') }}
    , attr.payer_attributed_provider
    , attr.payer_attributed_provider_practice
    , attr.payer_attributed_provider_organization
    , attr.payer_attributed_provider_lob
    , attr.custom_attributed_provider
    , attr.custom_attributed_provider_practice
    , attr.custom_attributed_provider_organization
    , attr.custom_attributed_provider_lob
    , attr.tuva_attributed_provider
    , attr.tuva_attributed_provider_bucket
    , attr.tuva_attributed_provider_specialty
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    , mm.data_source
from {{ ref('member_month__member_month') }} as mm
left outer join {{ ref('provider_attribution__member_month_attribution') }} as attr
  on mm.person_id = attr.person_id
  and mm.member_id = attr.member_id
  and mm.year_month = attr.year_month
  and mm.payer = attr.payer
  and mm.{{ quote_column('plan') }} = attr.{{ quote_column('plan') }}
  and mm.data_source = attr.data_source
