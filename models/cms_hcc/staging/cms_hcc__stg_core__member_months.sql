select
      person_id
    , year_month
    , payer
    , {{ quote_column('plan') }}
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__member_months') }}
