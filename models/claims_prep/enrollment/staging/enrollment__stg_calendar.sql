with calendar as (
    select *
    from {{ ref('tuva_data_assets', 'calendar') }}
)
select *
from calendar