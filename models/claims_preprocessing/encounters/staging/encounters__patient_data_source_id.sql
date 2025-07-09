with multiple_sources as (
    select
        member_id
        , data_source
    from {{ ref('normalized_input__medical_claim') }}
    union
    select
        member_id
        , data_source
    from {{ ref('normalized_input__eligibility') }}
)

select
    member_id
    , data_source
    , dense_rank() over (order by member_id, data_source) as patient_data_source_id
    , {{ dbt_utils.generate_surrogate_key(['data_source', 'member_id']) }} as member_data_source_sk
from multiple_sources
