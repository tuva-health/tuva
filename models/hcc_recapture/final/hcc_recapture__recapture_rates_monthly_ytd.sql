{{ config(
     enabled = var('hcc_recapture_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with ytd_hccs as (
select
      payer
    , payment_year
    , payment_year_month
    , closed_hccs as monthly_closed_hccs
    , open_hccs as monthly_open_hccs
    , total_hccs as monthly_total_hccs
    , recapture_rate as monthly_recapture_rate
    , sum(closed_hccs) over (partition by payer, payment_year order by payment_year_month rows between unbounded preceding and current row) as ytd_closed_hccs
    , sum(open_hccs) over (partition by payer, payment_year order by payment_year_month rows between unbounded preceding and current row) as ytd_open_hccs
    , sum(total_hccs) over (partition by payer, payment_year) as yearly_total_hccs
from {{ref('hcc_recapture__recapture_rates_monthly') }}
)

select
      payer
    , payment_year
    , payment_year_month
    , monthly_closed_hccs
    , monthly_open_hccs
    , monthly_total_hccs
    , monthly_recapture_rate
    , ytd_closed_hccs
    , ytd_open_hccs
    , yearly_total_hccs
    , ytd_closed_hccs / yearly_total_hccs as ytd_recapture_rate
from ytd_hccs
