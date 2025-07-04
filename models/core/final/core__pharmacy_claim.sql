select
    pharmacy_claim_sk
    , data_source
    , claim_id
    , claim_line_number
    , person_id
    , member_id
    , payer
    , {{ quote_column('plan') }}
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
    , file_name
    , file_date
    , ingest_datetime
    , {{ current_timestamp() }} as tuva_last_run
from {{ ref('the_tuva_project', 'core__stg_pharmacy_claim') }}
