with cte as (
select distinct
    encounter_group
    , encounter_type
from {{ ref('core__encounter') }}
)
select 
    *
    ,ROW_NUMBER() OVER (order by encounter_type) as encounter_type_sk
from cte