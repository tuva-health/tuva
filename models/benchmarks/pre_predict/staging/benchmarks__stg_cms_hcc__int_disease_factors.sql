select
    person_id
    , hcc_code
    , collection_end_date
from {{ ref('cms_hcc__int_disease_factors') }}
