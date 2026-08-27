{{ config(
     enabled = (
         the_tuva_project.tuva_boolean_var('provider_attribution_enabled', false) == True and
         the_tuva_project.tuva_boolean_var('claims_enabled', false)
     )
   )
}}

select
      person_id
    , member_id
    , year_month
    , payer
    , {{ the_tuva_project.quote_column('plan') }}
    , payer_attributed_provider
    , payer_attributed_provider_practice
    , payer_attributed_provider_organization
    , payer_attributed_provider_lob
    , custom_attributed_provider
    , custom_attributed_provider_practice
    , custom_attributed_provider_organization
    , custom_attributed_provider_lob
    , file_name
    , ingest_datetime
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__provider_attribution') }}
