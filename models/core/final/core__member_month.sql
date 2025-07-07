select
    member_month_key
    , data_source
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , year_month
    , month_start_date
    , month_end_date
    , {{ current_timestamp() }} as tuva_last_run
from {{ ref('core__stg_member_month') }}