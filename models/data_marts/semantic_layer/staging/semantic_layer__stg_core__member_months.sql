{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
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
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
FROM {{ ref('core__member_months') }} mm