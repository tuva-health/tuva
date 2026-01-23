{{ config(
     enabled = var('hcc_recapture_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with flatten_hccs as (
-- Remove claim ID and rendering NPI
select distinct
      person_id
    , payer
    , payment_year
    , date_from_parts(payment_year, month(recorded_date),1) as payment_year_month
    , recorded_date
    , model_version
    , hcc_code
    , gap_status
    , recapture_flag
    , row_number() over (partition by person_id, payer, payment_year, model_version, hcc_code order by recorded_date asc) as earliest_hcc_code
from {{ ref('hcc_recapture__hcc_status') }}
where 1=1
  and gap_status not in ('inappropriate for recapture', 'new')
  and gap_status is not null
  and suspect_hcc_flag = 0
)

, monthly_hcc_counts as (
select
      payer
    , payment_year
    , payment_year_month
    , sum(case when lower(gap_status) like '%closed%' then 1 else 0 end) as closed_hccs
    , sum(case when gap_status = 'open' then 1 else 0 end) as open_hccs
    , count(*) as total_hccs
from flatten_hccs
where earliest_hcc_code = 1
group by
      payer
    , payment_year
    , payment_year_month
)

select
      payer
    , payment_year
    , payment_year_month
    , closed_hccs
    , open_hccs
    , total_hccs
    , closed_hccs / total_hccs as recapture_rate
from monthly_hcc_counts
