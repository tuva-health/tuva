{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


select
    claim_id
    , data_source
    , column_name
    , normalized_code
    , normalized_description
    , occurrence_count
    , next_occurrence_count
    , occurrence_row_count
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('normalized_input__int_admit_type_voting') }}
where (occurrence_row_count = 1
        and occurrence_count > next_occurrence_count)
