with normalized_input__medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'normalized_input__medical_claim') }}
)
select
    medical_claim_sk
    , data_source
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , coalesce(claim_line_start_date, claim_start_date, admission_date) as inferred_claim_start_date
from normalized_input__medical_claim