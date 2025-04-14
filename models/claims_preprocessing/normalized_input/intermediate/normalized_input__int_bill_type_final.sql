

select
    claim_id
    , data_source
    , column_name
    , normalized_code
    , normalized_description
    , occurrence_count
    , occurrence_row_count
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('normalized_input__int_bill_type_voting') }}
where occurrence_row_count = 1
