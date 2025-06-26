{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

select
    person_id
    , sex
    , birth_date
    , state
    , race
from {{ ref('core__patient') }}
