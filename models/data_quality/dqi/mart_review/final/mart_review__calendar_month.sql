

select distinct
cast(year_month_int as {{ dbt.type_string() }}) as year_month
, full_date
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('reference_data__calendar') }} as c
where day = 1
