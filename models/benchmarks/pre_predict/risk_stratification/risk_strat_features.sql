{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

{#
  Risk stratification features built on top of the prospective person-year table.

  Design:
  - Keep all prior feature engineering (encounter counts, costs, fragmentation,
    recency, monthly rollups) and layer it on top of the base prospective table.
  - Use the prospective table's diagnosis_year as the lookback `year_nbr` for
    alignment with the 12m feature window; index month set to Dec of that year.
  - Enrollment-guarded joins replicate person_year style using member_months + calendar.

  Tunables:
  - High-cost monthly threshold: var('risk_strat_high_cost_threshold', 5000)
#}

{%- set highcost_threshold = var('risk_strat_high_cost_threshold', 5000) -%}
{%- set person_limit = var('risk_strat_person_limit', none) -%}

-- Optional subset of persons to accelerate dev/test runs
with subset_persons as (
  select p.person_id
  from {{ ref('core__patient') }} p
  {% if person_limit is not none %}
  limit {{ person_limit }}
  {% endif %}
)

, py as (
  select py.*
  from {{ ref('benchmarks__person_year_prospective') }} py
  inner join subset_persons sp on py.person_id = sp.person_id
)

, idx as (
  select
      py.person_id
    , py.data_source
    , py.payer
    , py.{{ quote_column('plan') }}
    -- Keying by prediction year, but computing features over diagnosis year
    , cast(py.prediction_year as {{ dbt.type_int() }})            as year_nbr
    , cast(py.prediction_year as {{ dbt.type_int() }})            as prediction_year
    , cast(py.diagnosis_year  as {{ dbt.type_int() }})            as diagnosis_year
    , cast(concat(py.diagnosis_year, '-12-01') as date)           as index_month
    , cast(concat(py.diagnosis_year, '12') as {{ dbt.type_int() }}) as index_year_month
    , cast(concat(py.diagnosis_year, '-12-31') as date)           as index_month_end_date
    -- Demographics (prior-year age; stable attrs direct)
    , case when py.prediction_year_age_at_year_start is not null
           then py.prediction_year_age_at_year_start - 1
           else null end                                          as age_at_year_start
    , py.prediction_year_sex                                      as sex
    , py.prediction_year_race                                     as race
    , py.prediction_year_state                                    as state
    -- Cold start
    , py.cold_start
    -- Lagged features only from prospective table
    {% set cols = adapter.get_columns_in_relation(ref('benchmarks__person_year_prospective')) %}
    {% for c in cols %}
      {% set n = c.name | lower %}
      {% if n.startswith('lag_') %}
    , py.{{ c.name }}
      {% endif %}
    {% endfor %}
    -- Target: future 12m paid amount
    , py.prediction_year_paid_amount                              as target_12m_paid_amount
    -- Targets: future 12m encounter counts
    , cast(
        coalesce(py.prediction_year_pmpc_emergency_department_count, 0)
        * coalesce(py.prediction_year_member_months, 0)
        as {{ dbt.type_int() }}
      )                                                           as target_12m_ed_encounter_count
    , cast(
        coalesce(py.prediction_year_pmpc_acute_inpatient_count, 0)
        * coalesce(py.prediction_year_member_months, 0)
        as {{ dbt.type_int() }}
      )                                                           as target_12m_acute_inpatient_encounter_count
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
    , sum(case when e.encounter_type = 'acute inpatient' then coalesce(mc.paid_amount,0) else 0 end) as cost_ip_12m
    , sum(case when e.encounter_type = 'emergency department' then coalesce(mc.paid_amount,0) else 0 end) as cost_ed_12m
    -- Encounter counts (12/9/6/3 months within year)
    , count(distinct case when e.encounter_type = 'acute inpatient' then e.encounter_id end) as ip_encounter_count_12m
    , count(distinct case when e.encounter_type = 'emergency department' then e.encounter_id end) as ed_encounter_count_12m
    , count(distinct case when e.encounter_type = 'acute inpatient' and cal.month >= 4 then e.encounter_id end) as ip_encounter_count_9m
    , count(distinct case when e.encounter_type = 'emergency department' and cal.month >= 4 then e.encounter_id end) as ed_encounter_count_9m
    , count(distinct case when e.encounter_type = 'acute inpatient' and cal.month >= 7 then e.encounter_id end) as ip_encounter_count_6m
    , count(distinct case when e.encounter_type = 'emergency department' and cal.month >= 7 then e.encounter_id end) as ed_encounter_count_6m
    , count(distinct case when e.encounter_type = 'acute inpatient' and cal.month >= 10 then e.encounter_id end) as ip_encounter_count_3m
    , count(distinct case when e.encounter_type = 'emergency department' and cal.month >= 10 then e.encounter_id end) as ed_encounter_count_3m
    -- Fragmentation
    , count(distinct mc.billing_id)  as distinct_providers_12m
    , count(distinct mc.facility_id) as distinct_facilities_12m
    -- Recency dates
    , max(case when e.encounter_type = 'acute inpatient' then e.encounter_start_date end) as last_ip_date
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

, enrolled_months as (
  -- Base set of enrolled member-months to ensure zero-cost months are included
  select
      mm.person_id
    , mm.data_source
    , mm.payer
    , mm.{{ quote_column('plan') }}
    , cal.year as year_nbr
    , mm.year_month
  from {{ ref('benchmarks__stg_core__member_months') }} as mm
  inner join subset_persons sp on mm.person_id = sp.person_id
  inner join {{ ref('benchmarks__stg_reference_data__calendar') }} as cal
    on mm.year_month = cal.year_month_int
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
  -- Drive monthly totals from enrolled months to include zero-cost months
  select
      b.person_id
    , b.data_source
    , b.payer
    , b.{{ quote_column('plan') }}
    , b.year_nbr
    , b.year_month
    , coalesce(med.med_paid_month, 0) + coalesce(rx.rx_paid_month, 0) as total_paid_month
  from enrolled_months b
  left join monthly_med med
    on b.person_id = med.person_id
   and b.data_source = med.data_source
   and b.payer = med.payer
   and b.{{ quote_column('plan') }} = med.{{ quote_column('plan') }}
   and b.year_nbr = med.year_nbr
   and b.year_month = med.year_month
  left join monthly_rx rx
    on b.person_id = rx.person_id
   and b.data_source = rx.data_source
   and b.payer = rx.payer
   and b.{{ quote_column('plan') }} = rx.{{ quote_column('plan') }}
   and b.year_nbr = rx.year_nbr
   and b.year_month = rx.year_month
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

, rolling_spend as (
  -- Rolling totals over the last 3/6/9 months of the diagnosis year
  select
      t.person_id
    , t.data_source
    , t.payer
    , t.{{ quote_column('plan') }}
    , t.year_nbr
    , sum(case when cal.month >= 10 then t.total_paid_month else 0 end) as total_paid_3m
    , sum(case when cal.month >= 7  then t.total_paid_month else 0 end) as total_paid_6m
    , sum(case when cal.month >= 4  then t.total_paid_month else 0 end) as total_paid_9m
  from monthly_totals t
  inner join {{ ref('benchmarks__stg_reference_data__calendar') }} as cal
    on t.year_month = cal.year_month_int
  group by t.person_id, t.data_source, t.payer, t.{{ quote_column('plan') }}, t.year_nbr
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
  -- Lookback encounter counts
  , coalesce(mca.ip_encounter_count_12m, 0) as lookback_ip_encounter_count_12m
  , coalesce(mca.ed_encounter_count_12m, 0) as lookback_ed_encounter_count_12m
  , coalesce(mca.ip_encounter_count_9m, 0)  as lookback_ip_encounter_count_9m
  , coalesce(mca.ed_encounter_count_9m, 0)  as lookback_ed_encounter_count_9m
  , coalesce(mca.ip_encounter_count_6m, 0)  as lookback_ip_encounter_count_6m
  , coalesce(mca.ed_encounter_count_6m, 0)  as lookback_ed_encounter_count_6m
  , coalesce(mca.ip_encounter_count_3m, 0)  as lookback_ip_encounter_count_3m
  , coalesce(mca.ed_encounter_count_3m, 0)  as lookback_ed_encounter_count_3m
  -- Lookback costs
  , coalesce(mca.cost_total_12m, 0) as lookback_med_cost_12m
  , coalesce(mca.cost_ip_12m, 0)    as lookback_med_ip_cost_12m
  , coalesce(mca.cost_ed_12m, 0)    as lookback_med_ed_cost_12m
  , coalesce(rx.cost_rx_12m, 0)     as lookback_rx_cost_12m
  , coalesce(ms.stdev_cost_monthly_12m, 0) as lookback_stdev_cost_monthly_12m
  , coalesce(mr.highcost_month_count_12m, 0) as lookback_highcost_month_count_12m
  -- Lookback rolling total spend over diagnosis year months (med+rx)
  , coalesce(rs.total_paid_3m, 0) as lookback_total_paid_3m
  , coalesce(rs.total_paid_6m, 0) as lookback_total_paid_6m
  , coalesce(rs.total_paid_9m, 0) as lookback_total_paid_9m
  , coalesce(mr.total_paid_12m, 0) as lookback_total_paid_12m
  -- Lookback fragmentation
  , coalesce(mca.distinct_providers_12m, 0)  as lookback_distinct_providers_12m
  , coalesce(mca.distinct_facilities_12m, 0) as lookback_distinct_facilities_12m
  -- Lookback recency in days to index_month_end_date
  , case
      when mca.last_ip_date is not null then {{ datediff('mca.last_ip_date', 'i.index_month_end_date', 'day') }}
      else null end as lookback_days_since_last_ip
  , case
      when mca.last_ed_date is not null then {{ datediff('mca.last_ed_date', 'i.index_month_end_date', 'day') }}
      else null end as lookback_days_since_last_ed
  , case when rh.last_highcost_date is not null
         then {{ datediff('rh.last_highcost_date', 'i.index_month_end_date', 'day') }}
         else null end as lookback_days_since_last_highcost
from idx i
left join mc_year_agg mca
  on i.person_id = mca.person_id
 and i.data_source = mca.data_source
 and i.payer = mca.payer
 and i.{{ quote_column('plan') }} = mca.{{ quote_column('plan') }}
 and i.diagnosis_year = mca.year_nbr
left join rx_costs rx
  on i.person_id = rx.person_id
 and i.data_source = rx.data_source
 and i.payer = rx.payer
 and i.{{ quote_column('plan') }} = rx.{{ quote_column('plan') }}
 and i.diagnosis_year = rx.year_nbr
left join monthly_rollups mr
  on i.person_id = mr.person_id
 and i.data_source = mr.data_source
 and i.payer = mr.payer
 and i.{{ quote_column('plan') }} = mr.{{ quote_column('plan') }}
 and i.diagnosis_year = mr.year_nbr
left join monthly_std ms
  on i.person_id = ms.person_id
 and i.data_source = ms.data_source
 and i.payer = ms.payer
 and i.{{ quote_column('plan') }} = ms.{{ quote_column('plan') }}
 and i.diagnosis_year = ms.year_nbr
left join rolling_spend rs
  on i.person_id = rs.person_id
 and i.data_source = rs.data_source
 and i.payer = rs.payer
 and i.{{ quote_column('plan') }} = rs.{{ quote_column('plan') }}
 and i.diagnosis_year = rs.year_nbr
left join recency_highcost rh
  on i.person_id = rh.person_id
 and i.data_source = rh.data_source
 and i.payer = rh.payer
 and i.{{ quote_column('plan') }} = rh.{{ quote_column('plan') }}
 and i.diagnosis_year = rh.year_nbr
