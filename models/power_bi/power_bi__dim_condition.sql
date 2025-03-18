with cte as (
SELECT DISTINCT
    condition
FROM {{ ref('chronic_conditions__tuva_chronic_conditions_long')}}
)
select 
    *
    ,ROW_NUMBER() OVER (order by condition) as condition_sk
from cte