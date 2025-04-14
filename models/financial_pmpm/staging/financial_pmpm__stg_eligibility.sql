
select
    person_id
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , {{ quote_column('plan') }}
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__eligibility') }}
