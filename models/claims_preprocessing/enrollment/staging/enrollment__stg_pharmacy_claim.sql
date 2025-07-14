with normalized_input__pharmacy_claim as (
    select *
    from {{ ref('the_tuva_project', 'normalized_input__pharmacy_claim') }}
)
select
    pharmacy_claim_sk
    , data_source
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , coalesce(paid_date, dispensing_date) as inferred_claim_start_date
from normalized_input__pharmacy_claim