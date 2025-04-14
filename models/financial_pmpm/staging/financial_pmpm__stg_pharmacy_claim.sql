

select
    person_id
    , dispensing_date
    , paid_date
    , paid_amount
    , allowed_amount
    , payer
    , {{ quote_column('plan') }}
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__pharmacy_claim') }}
