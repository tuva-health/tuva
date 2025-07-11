with encounters__int_office_visits__union as (
    select *
    from {{ ref('encounters__int_office_visits__union') }}
)
, encounters_ranked as (
    select *
        , row_number() over (partition by encounter_id order by priority_number) as relative_rank
    from encounters__int_office_visits__union
)
select *
from encounters_ranked
where relative_rank = 1