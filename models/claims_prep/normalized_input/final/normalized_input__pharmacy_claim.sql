with normalized_input__stg_pharmacy_claim as (
    select *
    from {{ ref('the_tuva_project', 'normalized_input__stg_pharmacy_claim') }}
)
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
    , round(paid_amount, 2) as paid_amount
    , round(allowed_amount, 2) as allowed_amount
    , round(charge_amount, 2) as charge_amount
    , round(coinsurance_amount, 2) as coinsurance_amount
    , round(copayment_amount, 2) as copayment_amount
    , round(deductible_amount, 2) as deductible_amount
    , in_network_flag
    , file_name
    , file_date
    , ingest_datetime
from normalized_input__stg_pharmacy_claim
