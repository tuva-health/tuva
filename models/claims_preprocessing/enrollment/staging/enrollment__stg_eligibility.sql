with normalized_input__eligibility as (
    select *
    from {{ ref('normalized_input__eligibility') }}
)
select
    eligibility_sk
    , data_source
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , enrollment_start_date
    , enrollment_end_date
from normalized_input__eligibility