{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT
    mm.person_id
  , mm.data_source
  , mm.year_month
  , mm.payer
  , mm.{{ quote_column('plan') }}
  , mm.payer_attributed_provider
  , mm.payer_attributed_provider_practice
  , mm.payer_attributed_provider_organization
  , mm.payer_attributed_provider_lob
  , mm.custom_attributed_provider
  , mm.custom_attributed_provider_practice
  , mm.custom_attributed_provider_organization
  , mm.custom_attributed_provider_lob
FROM {{ ref('core__member_months') }} mm