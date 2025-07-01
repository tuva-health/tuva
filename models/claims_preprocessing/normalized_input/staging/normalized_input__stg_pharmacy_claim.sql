select
    {{ dbt_utils.generate_surrogate_key(['data_source', 'claim_id', 'claim_line_number']) }} as surrogate_key
    , claim_id
    , claim_line_number
    , person_id
    , member_id
    , payer
    , plan
    , prescribing_provider_npi
    , dispensing_provider_npi
    , dispensing_date
    , ndc_code
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
    , data_source
    , file_name
    , file_date
    , ingest_datetime
from {{ ref('the_tuva_project', 'input_layer__pharmacy_claim') }}