{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

{%- set tuva_core_columns -%}
      member_month_key
    , person_id
    , member_id
    , year_month
    , payer
    , {{ quote_column('plan') }}
    , payer_attributed_provider
    , payer_attributed_provider_practice
    , payer_attributed_provider_organization
    , payer_attributed_provider_lob
    , custom_attributed_provider
    , custom_attributed_provider_practice
    , custom_attributed_provider_organization
    , custom_attributed_provider_lob
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , data_source
    , tuva_last_run
{%- endset %}

select
    {{ tuva_core_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_claims_member_months') }}
