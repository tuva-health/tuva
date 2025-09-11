{# {{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}} #}

select
    person_id
    , year_month
    , payer
    , {{ quote_column('plan') }}
    , data_source
from {{ ref('core__member_months') }}
