-- Ensure the latest YTD value matches the recapture rates
select
      ytd.payer
    , ytd.payment_year
from {{ref('ra_ops__recapture_rates_monthly_ytd')}} ytd
left join {{ref('ra_ops__recapture_rates')}} recap
    on  ytd.payer = recap.payer
    and ytd.payment_year = recap.payment_year
    and ytd.ytd_recapture_rate = recap.recapture_rate
where 
    month(ytd.payment_year_month) = 12
    and recap.payer is null
