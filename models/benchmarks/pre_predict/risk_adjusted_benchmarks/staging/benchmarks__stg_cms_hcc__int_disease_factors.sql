{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

select
    person_id
    , hcc_code
    , collection_end_date
from {{ ref('cms_hcc__int_disease_factors') }}
