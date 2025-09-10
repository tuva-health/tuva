{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

{#
  Risk stratification features built on top of the flat `benchmarks__person_year` table.

  Design:
  - One row per person-year (same grain as person_year), with an explicit index_month at Dec of year_nbr
  - 12m lookback features are aligned to the calendar year of `year_nbr` (Jan..Dec)
  - Enrollment-guarded joins replicate person_year style using member_months + calendar
  - Encounter counts sourced from core encounter (joined to medical_claim for payer/plan/data_source)
  - Distinct providers/facilities from core medical claim
  - Pharmacy costs from core pharmacy claims (paid_date fallback to dispensing_date)

  Tunables:
  - High-cost monthly threshold: var('risk_strat_high_cost_threshold', 5000)
#}

{%- set highcost_threshold = var('risk_strat_high_cost_threshold', 5000) -%}
{%- set person_limit = var('risk_strat_person_limit', none) -%}

-- Optional subset of persons to accelerate dev/test runs
, subset_persons as (
  select p.person_id
  from {{ ref('core__patient') }} p
  {% if person_limit is not none %}
  limit {{ person_limit }}
  {% endif %}
)

, py as (
  select py.*
  from {{ ref('benchmarks__person_year') }} py
  inner join subset_persons sp on py.person_id = sp.person_id
)

, idx as (
  select
      py.*
    , cast(concat(py.year_nbr, '-12-01') as date)           as index_month
    , cast(concat(py.year_nbr, '12') as {{ dbt.type_int() }}) as index_year_month
    , cast(concat(py.year_nbr, '-12-31') as date)            as index_month_end_date
  from py
)

-- Use calendar joins for cross‑DB friendly year and month derivation.

, mc_year_agg as (
  -- Medical-claim anchored aggregation joined to encounters; enrollment-gated; year and month via calendar
  select
      mc.person_id
    , mc.data_source
    , mc.payer
    , mc.{{ quote_column('plan') }}
    , cal.year as year_nbr
    -- Paid amounts
    , sum(coalesce(mc.paid_amount,0)) as cost_total_12m
    , sum(case when e.encounter_group = 'inpatient' then coalesce(mc.paid_amount,0) else 0 end) as cost_ip_12m
    , sum(case when e.encounter_type = 'emergency department' then coalesce(mc.paid_amount,0) else 0 end) as cost_ed_12m
    -- Encounter counts (12/9/6/3 months within year)
    , count(distinct case when e.encounter_group = 'inpatient' then e.encounter_id end) as ip_encounter_count_12m
    , count(distinct case when e.encounter_type = 'emergency department' then e.encounter_id end) as ed_encounter_count_12m
    , count(distinct case when e.encounter_group = 'inpatient' and cal.month >= 4 then e.encounter_id end) as ip_encounter_count_9m
    , count(distinct case when e.encounter_type = 'emergency department' and cal.month >= 4 then e.encounter_id end) as ed_encounter_count_9m
    , count(distinct case when e.encounter_group = 'inpatient' and cal.month >= 7 then e.encounter_id end) as ip_encounter_count_6m
    , count(distinct case when e.encounter_type = 'emergency department' and cal.month >= 7 then e.encounter_id end) as ed_encounter_count_6m
    , count(distinct case when e.encounter_group = 'inpatient' and cal.month >= 10 then e.encounter_id end) as ip_encounter_count_3m
    , count(distinct case when e.encounter_type = 'emergency department' and cal.month >= 10 then e.encounter_id end) as ed_encounter_count_3m
    -- Fragmentation
    , count(distinct mc.billing_id)  as distinct_providers_12m
    , count(distinct mc.facility_id) as distinct_facilities_12m
    -- Recency dates
    , max(case when e.encounter_group = 'inpatient' then e.encounter_start_date end) as last_ip_date
    , max(case when e.encounter_type = 'emergency department' then e.encounter_start_date end) as last_ed_date
  from {{ ref('benchmarks__stg_core__medical_claim') }} as mc
  inner join subset_persons sp on mc.person_id = sp.person_id
  inner join {{ ref('benchmarks__stg_core__encounter') }} as e
    on e.encounter_id = mc.encounter_id
  inner join {{ ref('benchmarks__stg_reference_data__calendar') }} as cal
    on e.encounter_start_date = cal.full_date
  inner join {{ ref('benchmarks__stg_core__member_months') }} as mm
    on mc.person_id = mm.person_id
   and mc.data_source = mm.data_source
   and mc.payer = mm.payer
   and mc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
   and cal.year_month_int = mm.year_month
  group by mc.person_id, mc.data_source, mc.payer, mc.{{ quote_column('plan') }}, cal.year
)

, rx_costs as (
  -- Pharmacy claim paid amounts over the lookback year
  select
      pc.person_id
    , pc.data_source
    , pc.payer
    , pc.{{ quote_column('plan') }}
    , cal.year as year_nbr
    , sum(coalesce(pc.paid_amount,0)) as cost_rx_12m
  from {{ ref('core__pharmacy_claim') }} as pc
  inner join subset_persons sp on pc.person_id = sp.person_id
  inner join {{ ref('benchmarks__stg_reference_data__calendar') }} as cal
    on coalesce(pc.paid_date, pc.dispensing_date) = cal.full_date
  inner join {{ ref('benchmarks__stg_core__member_months') }} as mm
    on pc.person_id = mm.person_id
   and pc.data_source = mm.data_source
   and pc.payer = mm.payer
   and pc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
   and cal.year_month_int = mm.year_month
  group by pc.person_id, pc.data_source, pc.payer, pc.{{ quote_column('plan') }}, cal.year
)

, monthly_med as (
  -- Monthly total medical costs (for stdev and high-cost month logic)
  select
      mc.person_id, mc.data_source, mc.payer, mc.{{ quote_column('plan') }}
    , cal.year as year_nbr
    , cal.year_month_int as year_month
    , sum(coalesce(mc.paid_amount,0)) as med_paid_month
  from {{ ref('benchmarks__stg_core__medical_claim') }} as mc
  inner join subset_persons sp on mc.person_id = sp.person_id
  inner join {{ ref('benchmarks__stg_reference_data__calendar') }} as cal
    on coalesce(mc.claim_end_date, mc.claim_start_date) = cal.full_date
  inner join {{ ref('benchmarks__stg_core__member_months') }} as mm
    on mc.person_id = mm.person_id
   and mc.data_source = mm.data_source
   and mc.payer = mm.payer
   and mc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
   and cal.year_month_int = mm.year_month
  group by mc.person_id, mc.data_source, mc.payer, mc.{{ quote_column('plan') }}, cal.year, cal.year_month_int
)

, monthly_rx as (
  select
      pc.person_id, pc.data_source, pc.payer, pc.{{ quote_column('plan') }}
    , cal.year as year_nbr
    , cal.year_month_int as year_month
    , sum(coalesce(pc.paid_amount,0)) as rx_paid_month
  from {{ ref('core__pharmacy_claim') }} as pc
  inner join subset_persons sp on pc.person_id = sp.person_id
  inner join {{ ref('benchmarks__stg_reference_data__calendar') }} as cal
    on coalesce(pc.paid_date, pc.dispensing_date) = cal.full_date
  inner join {{ ref('benchmarks__stg_core__member_months') }} as mm
    on pc.person_id = mm.person_id
   and pc.data_source = mm.data_source
   and pc.payer = mm.payer
   and pc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
   and cal.year_month_int = mm.year_month
  group by pc.person_id, pc.data_source, pc.payer, pc.{{ quote_column('plan') }}, cal.year, cal.year_month_int
)

, monthly_totals as (
  select
      coalesce(med.person_id, rx.person_id) as person_id
    , coalesce(med.data_source, rx.data_source) as data_source
    , coalesce(med.payer, rx.payer) as payer
    , coalesce(med.{{ quote_column('plan') }}, rx.{{ quote_column('plan') }}) as {{ quote_column('plan') }}
    , coalesce(med.year_nbr, rx.year_nbr) as year_nbr
    , coalesce(med.year_month, rx.year_month) as year_month
    , coalesce(med.med_paid_month, 0) + coalesce(rx.rx_paid_month, 0) as total_paid_month
  from monthly_med med
  full outer join monthly_rx rx
    on med.person_id = rx.person_id
   and med.data_source = rx.data_source
   and med.payer = rx.payer
   and med.{{ quote_column('plan') }} = rx.{{ quote_column('plan') }}
   and med.year_nbr = rx.year_nbr
   and med.year_month = rx.year_month
)

, monthly_rollups as (
  select
      person_id, data_source, payer, {{ quote_column('plan') }}, year_nbr
    , count(*) as observed_months
    , sum(total_paid_month) as total_paid_12m
    , sum(case when total_paid_month > {{ highcost_threshold }} then 1 else 0 end) as highcost_month_count_12m
    , {{ dbt.safe_cast('null', 'float') }} as stdev_cost_monthly_12m_placeholder
  from monthly_totals
  group by person_id, data_source, payer, {{ quote_column('plan') }}, year_nbr
)

, monthly_std as (
  -- Compute standard deviation of monthly totals across the lookback months
  select
      person_id, data_source, payer, {{ quote_column('plan') }}, year_nbr
    , stddev_samp(total_paid_month) as stdev_cost_monthly_12m
  from monthly_totals
  group by person_id, data_source, payer, {{ quote_column('plan') }}, year_nbr
)



, recency_highcost as (
  -- Last day of the last high-cost month in the lookback window
  select
      t.person_id, t.data_source, t.payer, t.{{ quote_column('plan') }}, t.year_nbr
    , max(case when t.total_paid_month > {{ highcost_threshold }} then cal.last_day_of_month end) as last_highcost_date
  from monthly_totals t
  inner join {{ ref('benchmarks__stg_reference_data__calendar') }} as cal
    on t.year_month = cal.year_month_int
  group by t.person_id, t.data_source, t.payer, t.{{ quote_column('plan') }}, t.year_nbr
)

select
  i.*
  -- Counts
  , coalesce(mca.ip_encounter_count_12m, 0) as ip_encounter_count_12m
  , coalesce(mca.ed_encounter_count_12m, 0) as ed_encounter_count_12m
  , coalesce(mca.ip_encounter_count_9m, 0)  as ip_encounter_count_9m
  , coalesce(mca.ed_encounter_count_9m, 0)  as ed_encounter_count_9m
  , coalesce(mca.ip_encounter_count_6m, 0)  as ip_encounter_count_6m
  , coalesce(mca.ed_encounter_count_6m, 0)  as ed_encounter_count_6m
  , coalesce(mca.ip_encounter_count_3m, 0)  as ip_encounter_count_3m
  , coalesce(mca.ed_encounter_count_3m, 0)  as ed_encounter_count_3m
  -- Costs
  , coalesce(mca.cost_total_12m, 0) as cost_total_12m
  , coalesce(mca.cost_ip_12m, 0)    as cost_ip_12m
  , coalesce(mca.cost_ed_12m, 0)    as cost_ed_12m
  , coalesce(rx.cost_rx_12m, 0)    as cost_rx_12m
  , coalesce(ms.stdev_cost_monthly_12m, 0) as stdev_cost_monthly_12m
  , coalesce(mr.highcost_month_count_12m, 0) as highcost_month_count_12m
  -- Fragmentation
  , coalesce(mca.distinct_providers_12m, 0)  as distinct_providers_12m
  , coalesce(mca.distinct_facilities_12m, 0) as distinct_facilities_12m
  -- Recency in days to index_month_end_date
  , case
      when mca.last_ip_date is not null then {{ datediff('mca.last_ip_date', 'i.index_month_end_date', 'day') }}
      else null end as days_since_last_ip
  , case
      when mca.last_ed_date is not null then {{ datediff('mca.last_ed_date', 'i.index_month_end_date', 'day') }}
      else null end as days_since_last_ed
  , case when rh.last_highcost_date is not null
         then {{ datediff('rh.last_highcost_date', 'i.index_month_end_date', 'day') }}
         else null end as days_since_last_highcost
from idx i
left join mc_year_agg mca
  on i.person_id = mca.person_id
 and i.data_source = mca.data_source
 and i.payer = mca.payer
 and i.{{ quote_column('plan') }} = mca.{{ quote_column('plan') }}
 and i.year_nbr = mca.year_nbr
left join rx_costs rx
  on i.person_id = rx.person_id
 and i.data_source = rx.data_source
 and i.payer = rx.payer
 and i.{{ quote_column('plan') }} = rx.{{ quote_column('plan') }}
 and i.year_nbr = rx.year_nbr
left join monthly_rollups mr
  on i.person_id = mr.person_id
 and i.data_source = mr.data_source
 and i.payer = mr.payer
 and i.{{ quote_column('plan') }} = mr.{{ quote_column('plan') }}
 and i.year_nbr = mr.year_nbr
left join monthly_std ms
  on i.person_id = ms.person_id
 and i.data_source = ms.data_source
 and i.payer = ms.payer
 and i.{{ quote_column('plan') }} = ms.{{ quote_column('plan') }}
 and i.year_nbr = ms.year_nbr
left join recency_highcost rh
  on i.person_id = rh.person_id
 and i.data_source = rh.data_source
 and i.payer = rh.payer
 and i.{{ quote_column('plan') }} = rh.{{ quote_column('plan') }}
 and i.year_nbr = rh.year_nbr


/*  guidance
You want 12-month lookback features and 12-month future outcomes.

With data spanning Jan-2023 → Dec-2024, the only index month that fits 12m lookback + 12m future entirely inside the data is Dec-2023 (lookback: Jan-2023→Dec-2023; future: Jan-2024→Dec-2024).
→ That gives you one row per member for the 12→12 setup. the person year table is already set up at this grain. we just need to add a index_month column.


 want to add these features for risk stratification models
Costs: cost_total_*m, cost_ip_*m, cost_ed_*m, cost_rx_*m, stdev_cost_monthly_*m, highcost_month_count_*m (>$X)

Recency: days_since_last_ip, days_since_last_ed, days_since_last_highcost

Fragmentation: distinct_providers_12m, distinct_facilities_12m

*/
