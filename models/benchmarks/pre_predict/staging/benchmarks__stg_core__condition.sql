{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

select
    person_id
    , normalized_code
    , recorded_date
from {{ ref('core__condition') }}
