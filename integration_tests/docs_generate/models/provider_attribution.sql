

select
      patient_id
    , year_month
    , payer
    , plan
    , data_source
    , payer_attributed_provider
    , payer_attributed_provider_practice
    , payer_attributed_provider_organization
    , payer_attributed_provider_lob
    , custom_attributed_provider
    , custom_attributed_provider_practice
    , custom_attributed_provider_organization
    , custom_attributed_provider_lob
from {{ ref('provider_attribution_seed') }}
