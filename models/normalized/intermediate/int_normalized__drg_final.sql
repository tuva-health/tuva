{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
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
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('int_normalized__drg_voting') }}
where (occurrence_row_count = 1
        and occurrence_count > next_occurrence_count)
