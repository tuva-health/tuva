select mc.*,
cast(c.year_month_int as varchar(6)) as year_month
FROM {{ ref('core__pharmacy_claim')}}  mc
left join {{ ref('data_quality__dqi_calendar') }} c on coalesce(mc.paid_date,mc.dispensing_date) = c.full_date
