{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

with flatten_hccs as (
-- Remove claim ID and rendering NPI
select distinct
      person_id
    , payer
    , payment_year
    , cast({{ concat_custom([
            "payment_year"
          , "'-'"
          , date_part('month', 'recorded_date')
          , "'-'"
          , "'1'"
        ]) }} as date) as payment_year_month
    , recorded_date
    , model_version
    , hcc_code
    , gap_status
    , case when hcc_type = 'suspect' then 1 else 0 end as suspect_hcc_flag
    , recapturable_flag
    , row_number() over (partition by person_id, payer, payment_year, model_version, hcc_code 
                        order by case hcc_type when 'coded' then 1 when 'captured' then 2 else 3 end, recorded_date asc) as earliest_hcc_code
from {{ ref('hcc_recapture__hcc_status')}}
where 1=1
  and gap_status not in ('ineligible for recapture', 'new')
  and hcc_type in ('captured', 'coded', 'suspect')
  and recapturable_flag = 1
  and filtered_by_hierarchy_flag = 0
)

, monthly_hcc_counts as (
select
      payer
    , payment_year
    , payment_year_month
    , suspect_hcc_flag
    , sum(case when lower(gap_status) like '%closed%' then 1 else 0 end) as closed_hccs
    , sum(case when gap_status = 'open' then 1 else 0 end) as open_hccs
    , count(*) as total_hccs
from flatten_hccs
where earliest_hcc_code = 1
group by
      payer
    , payment_year
    , payment_year_month
    , suspect_hcc_flag
)

, no_suspects_recap_rate as (
  select 
      payer
    , payment_year
    , payment_year_month   
    , sum(closed_hccs) as closed_hccs
    , sum(open_hccs) as open_hccs
    , sum(total_hccs) as total_hccs
    , sum(closed_hccs) / sum(total_hccs) as recapture_rate
  from monthly_hcc_counts
  where suspect_hcc_flag = 0
  group by
      payer
    , payment_year
    , payment_year_month     
)

, all_recap_rate as (
select
      payer
    , payment_year
    , payment_year_month   
    , sum(closed_hccs) as closed_hccs
    , sum(open_hccs) as open_hccs
    , sum(total_hccs) as total_hccs
    , sum(closed_hccs) / sum(total_hccs) as recapture_rate
from monthly_hcc_counts hcc
group by
      payer
    , payment_year
    , payment_year_month
)

select
  recap.payer
  , recap.payment_year
  , recap.payment_year_month
  , nosus.closed_hccs as no_suspects_closed_hccs
  , nosus.open_hccs as no_suspects_open_hccs
  , nosus.total_hccs as no_suspects_total_hccs
  , nosus.recapture_rate as no_suspects_recapture_rate
  , recap.closed_hccs
  , recap.open_hccs
  , recap.total_hccs
  , recap.recapture_rate
from all_recap_rate recap
left join no_suspects_recap_rate nosus
  on recap.payer = nosus.payer
  and recap.payment_year = nosus.payment_year
  and recap.payment_year_month = nosus.payment_year_month