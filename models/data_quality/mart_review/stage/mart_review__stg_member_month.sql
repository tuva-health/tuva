with cte as 
(
select distinct year_month_int
,full_date
from {{ ref('data_quality__dqi_calendar') }} c
where day = 1

)

select  mm.*
,c.year_month_int
,c.full_date as year_month_date
FROM {{ ref('core__member_months')}} mm
left join cte c on cast(mm.year_month as int) = c.year_month_int