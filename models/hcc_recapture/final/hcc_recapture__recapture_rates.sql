{{ config(
     enabled = var('hcc_recapture_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))) | as_bool
}}

with monthly_hcc_counts as (
select
      payer
    , payment_year
    , sum(closed_hccs) as closed_hccs
    , sum(open_hccs) as open_hccs
    , sum(total_hccs) as total_hccs
from {{ ref('hcc_recapture__recapture_rates_monthly') }}
group by
      payer
    , payment_year
)

select
      payer
    , payment_year
    , closed_hccs
    , open_hccs
    , total_hccs
    , closed_hccs / total_hccs as recapture_rate
from monthly_hcc_counts
