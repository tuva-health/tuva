select
    claim_id
    , data_source
    , column_name
    , normalized_code
    , occurrence_count
    , next_occurrence_count
    , occurrence_row_count
from {{ ref('header_validation__int_apr_drg_voting') }}
where (occurrence_row_count = 1
        and occurrence_count > next_occurrence_count)