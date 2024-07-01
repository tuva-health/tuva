select distinct 
cast(year_month_int as varchar(6)) as year_month
, full_date 
from {{ ref('data_quality__dqi_calendar') }} c
where day = 1