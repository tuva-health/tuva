{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_core_columns -%}
      pharmacy_claim_id
    , claim_id
    , claim_line_number
    , person_id
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , prescribing_provider_id
    , prescribing_provider_name
    , dispensing_provider_id
    , dispensing_provider_name
    , dispensing_date
    , ndc_code
    , ndc_description
    , quantity
    , days_supply
    , refills
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , in_network_flag
    , enrollment_flag
    , member_month_key
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__pharmacy_claim')) }}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , data_source
    , file_date
    , file_name
    , tuva_last_run
{%- endset %}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_claims_pharmacy_claim') }}
