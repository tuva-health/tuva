{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

{#
  Person-month grain risk stratification features with rolling 12-month lookback
  and forward targets. Reuses annual feature pivots (conditions, CMS, HCCs)
  by joining them to the calendar year that contains the index month.
#}

{% set highcost_threshold = var('risk_strat_high_cost_threshold', 5000) %}
{% set person_limit = var('risk_strat_person_limit', none) %}

{% set src = ref('benchmarks__person_year_prospective') %}
{% set cols = adapter.get_columns_in_relation(src) %}

{% set cond_cols = [] %}
{% set cms_cols = [] %}
{% set hcc_cols = [] %}

{% for c in cols %}
  {% set n = c.name | lower %}
  {% if n.startswith('cond_') %}
    {% do cond_cols.append(n) %}
  {% elif n.startswith('cms_') %}
    {% do cms_cols.append(n) %}
  {% elif n.startswith('hcc_') %}
    {% do hcc_cols.append(n) %}
  {% endif %}
{% endfor %}

with subset_persons as (
  select distinct p.person_id
  from {{ ref('core__patient') }} p
  {% if person_limit is not none %}
  limit {{ person_limit }}
  {% endif %}
)

,cal as (
  select distinct year_month_int
  ,year
  ,first_day_of_month
  ,last_day_of_month
  from {{ ref('benchmarks__stg_reference_data__calendar') }}
)

, member_months as (
  select
      mm.person_id
    , mm.data_source
    , mm.payer
    , mm.{{ quote_column('plan') }}
    , mm.year_month as index_year_month
    , cal.year as year_nbr
    , cal.year as diagnosis_year
    , cal.first_day_of_month as index_month
    , cal.last_day_of_month as index_month_end_date
    , {{ dateadd('month', -11, 'cal.first_day_of_month') }} as lookback_start_date
    , {{ dateadd('day', 1, 'cal.last_day_of_month') }} as prediction_start_date
    , {{ dateadd('month', 12, 'cal.last_day_of_month') }} as prediction_end_date
    , cal.year + 1 as prediction_year
  from {{ ref('benchmarks__stg_core__member_months') }} mm
  inner join subset_persons sp on mm.person_id = sp.person_id
  inner join cal
    on mm.year_month = cal.year_month_int
)

, idx as (
  select
      mm.person_id
    , mm.data_source
    , mm.payer
    , mm.{{ quote_column('plan') }}
    , mm.index_year_month
    , mm.year_nbr
    , mm.diagnosis_year
    , mm.prediction_year
    , mm.index_month
    , mm.index_month_end_date
    , mm.lookback_start_date
    , mm.prediction_start_date
    , mm.prediction_end_date
    , p.sex
    , p.race
    , p.state
    , case
        when p.birth_date is not null then {{ datediff('p.birth_date', 'mm.index_month', 'year') }}
        else null
      end as age_at_year_start
  from member_months mm
  inner join subset_persons sp on mm.person_id = sp.person_id
  left join {{ ref('benchmarks__stg_core__patient') }} p
    on mm.person_id = p.person_id
)

, mc_enriched as (
  select
      mc.person_id
    , mc.data_source
    , mc.payer
    , mc.{{ quote_column('plan') }}
    , mc.billing_id
    , mc.facility_id
    , coalesce(mc.paid_amount, 0) as paid_amount
    , e.encounter_id
    , e.encounter_type
    , e.encounter_group
    , e.encounter_start_date
    , cal.year_month_int as year_month
    , cal.first_day_of_month as month_start
    , cal.last_day_of_month as month_end
  from {{ ref('benchmarks__stg_core__medical_claim') }} mc
  inner join subset_persons sp on mc.person_id = sp.person_id
  inner join {{ ref('benchmarks__stg_core__encounter') }} e
    on e.encounter_id = mc.encounter_id
  inner join {{ ref('benchmarks__stg_reference_data__calendar') }} cal
    on e.encounter_start_date = cal.full_date
  inner join {{ ref('benchmarks__stg_core__member_months') }} mm
    on mc.person_id = mm.person_id
   and mc.data_source = mm.data_source
   and mc.payer = mm.payer
   and mc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
   and cal.year_month_int = mm.year_month
)

, mc_monthly as (
  select
      mc.person_id
    , mc.data_source
    , mc.payer
    , mc.{{ quote_column('plan') }}
    , mc.year_month
    , mc.month_start
    , mc.month_end
    , sum(mc.paid_amount) as med_paid_month
    , sum(case when mc.encounter_type = 'acute inpatient' then mc.paid_amount else 0 end) as med_ip_paid_month
    , sum(case when mc.encounter_type = 'emergency department' then mc.paid_amount else 0 end) as med_ed_paid_month
    , count(distinct case when mc.encounter_type = 'acute inpatient' then mc.encounter_id end) as ip_encounter_count_month
    , count(distinct case when mc.encounter_type = 'emergency department' then mc.encounter_id end) as ed_encounter_count_month
    , count(distinct case when mc.billing_id is not null then mc.billing_id end) as provider_count_month
    , count(distinct case when mc.facility_id is not null then mc.facility_id end) as facility_count_month
    , max(case when mc.encounter_type = 'acute inpatient' then mc.encounter_start_date end) as last_ip_date_in_month
    , max(case when mc.encounter_type = 'emergency department' then mc.encounter_start_date end) as last_ed_date_in_month
  from mc_enriched mc
  inner join subset_persons sp on mc.person_id = sp.person_id
  group by mc.person_id, mc.data_source, mc.payer, mc.{{ quote_column('plan') }}, mc.year_month, mc.month_start, mc.month_end
)

, rx_monthly as (
  select
      pc.person_id
    , pc.data_source
    , pc.payer
    , pc.{{ quote_column('plan') }}
    , cal.year_month_int as year_month
    , cal.first_day_of_month as month_start
    , cal.last_day_of_month as month_end
    , sum(coalesce(pc.paid_amount, 0)) as rx_paid_month
  from {{ ref('core__pharmacy_claim') }} pc
  inner join subset_persons sp on pc.person_id = sp.person_id
  inner join {{ ref('benchmarks__stg_reference_data__calendar') }} cal
    on coalesce(pc.paid_date, pc.dispensing_date) = cal.full_date
  inner join {{ ref('benchmarks__stg_core__member_months') }} mm
    on pc.person_id = mm.person_id
   and pc.data_source = mm.data_source
   and pc.payer = mm.payer
   and pc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
   and cal.year_month_int = mm.year_month
  group by pc.person_id, pc.data_source, pc.payer, pc.{{ quote_column('plan') }}, cal.year_month_int, cal.first_day_of_month, cal.last_day_of_month
)

, monthly_totals as (
  select
      idx.person_id
    , idx.data_source
    , idx.payer
    , idx.{{ quote_column('plan') }}
    , idx.index_year_month as year_month
    , idx.index_month as month_start
    , idx.index_month_end_date as month_end
    , coalesce(mc.med_paid_month, 0) as med_paid_month
    , coalesce(mc.med_ip_paid_month, 0) as med_ip_paid_month
    , coalesce(mc.med_ed_paid_month, 0) as med_ed_paid_month
    , coalesce(rx.rx_paid_month, 0) as rx_paid_month
    , coalesce(mc.ip_encounter_count_month, 0) as ip_encounter_count_month
    , coalesce(mc.ed_encounter_count_month, 0) as ed_encounter_count_month
    , coalesce(mc.provider_count_month, 0) as provider_count_month
    , coalesce(mc.facility_count_month, 0) as facility_count_month
    , mc.last_ip_date_in_month
    , mc.last_ed_date_in_month
    , coalesce(mc.med_paid_month, 0) + coalesce(rx.rx_paid_month, 0) as total_paid_month
  from idx
  inner join subset_persons sp on idx.person_id = sp.person_id
  left join mc_monthly mc
    on idx.person_id = mc.person_id
   and idx.data_source = mc.data_source
   and idx.payer = mc.payer
   and idx.{{ quote_column('plan') }} = mc.{{ quote_column('plan') }}
   and idx.index_year_month = mc.year_month
  left join rx_monthly rx
    on idx.person_id = rx.person_id
   and idx.data_source = rx.data_source
   and idx.payer = rx.payer
   and idx.{{ quote_column('plan') }} = rx.{{ quote_column('plan') }}
   and idx.index_year_month = rx.year_month
)

, lookback_future_metrics as (
  select
      cur.person_id
    , cur.data_source
    , cur.payer
    , cur.{{ quote_column('plan') }}
    , cur.year_month
    , sum(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start then hist.ip_encounter_count_month else 0 end) as lookback_ip_encounter_count_12m
    , sum(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start then hist.ed_encounter_count_month else 0 end) as lookback_ed_encounter_count_12m
    , sum(case when hist.month_start between {{ dateadd('month', -8, 'cur.month_start') }} and cur.month_start then hist.ip_encounter_count_month else 0 end) as lookback_ip_encounter_count_9m
    , sum(case when hist.month_start between {{ dateadd('month', -8, 'cur.month_start') }} and cur.month_start then hist.ed_encounter_count_month else 0 end) as lookback_ed_encounter_count_9m
    , sum(case when hist.month_start between {{ dateadd('month', -5, 'cur.month_start') }} and cur.month_start then hist.ip_encounter_count_month else 0 end) as lookback_ip_encounter_count_6m
    , sum(case when hist.month_start between {{ dateadd('month', -5, 'cur.month_start') }} and cur.month_start then hist.ed_encounter_count_month else 0 end) as lookback_ed_encounter_count_6m
    , sum(case when hist.month_start between {{ dateadd('month', -2, 'cur.month_start') }} and cur.month_start then hist.ip_encounter_count_month else 0 end) as lookback_ip_encounter_count_3m
    , sum(case when hist.month_start between {{ dateadd('month', -2, 'cur.month_start') }} and cur.month_start then hist.ed_encounter_count_month else 0 end) as lookback_ed_encounter_count_3m
    , sum(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start then hist.med_paid_month else 0 end) as lookback_med_cost_12m
    , sum(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start then hist.med_ip_paid_month else 0 end) as lookback_med_ip_cost_12m
    , sum(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start then hist.med_ed_paid_month else 0 end) as lookback_med_ed_cost_12m
    , sum(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start then hist.rx_paid_month else 0 end) as lookback_rx_cost_12m
    , sum(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start then hist.total_paid_month else 0 end) as lookback_total_paid_12m
    , sum(case when hist.month_start between {{ dateadd('month', -8, 'cur.month_start') }} and cur.month_start then hist.total_paid_month else 0 end) as lookback_total_paid_9m
    , sum(case when hist.month_start between {{ dateadd('month', -5, 'cur.month_start') }} and cur.month_start then hist.total_paid_month else 0 end) as lookback_total_paid_6m
    , sum(case when hist.month_start between {{ dateadd('month', -2, 'cur.month_start') }} and cur.month_start then hist.total_paid_month else 0 end) as lookback_total_paid_3m
    , sum(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start and hist.total_paid_month > {{ highcost_threshold }} then 1 else 0 end) as lookback_highcost_month_count_12m
    , stddev_samp(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start then hist.total_paid_month end) as lookback_stdev_cost_monthly_12m
    , max(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start then hist.last_ip_date_in_month end) as last_ip_date_window
    , max(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start then hist.last_ed_date_in_month end) as last_ed_date_window
    , max(case when hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and cur.month_start and hist.total_paid_month > {{ highcost_threshold }} then hist.month_end end) as last_highcost_month_end
    , sum(case when hist.month_start > cur.month_start and hist.month_start <= {{ dateadd('month', 12, 'cur.month_start') }} then hist.total_paid_month else 0 end) as target_12m_paid_amount
    , sum(case when hist.month_start > cur.month_start and hist.month_start <= {{ dateadd('month', 12, 'cur.month_start') }} then hist.ed_encounter_count_month else 0 end) as target_12m_ed_encounter_count
    , sum(case when hist.month_start > cur.month_start and hist.month_start <= {{ dateadd('month', 12, 'cur.month_start') }} then hist.ip_encounter_count_month else 0 end) as target_12m_acute_inpatient_encounter_count
  from monthly_totals cur
  inner join subset_persons sp on cur.person_id = sp.person_id
  left join monthly_totals hist
    on cur.person_id = hist.person_id
   and cur.data_source = hist.data_source
   and cur.payer = hist.payer
   and cur.{{ quote_column('plan') }} = hist.{{ quote_column('plan') }}
   and hist.month_start between {{ dateadd('month', -11, 'cur.month_start') }} and {{ dateadd('month', 12, 'cur.month_start') }}
  group by cur.person_id, cur.data_source, cur.payer, cur.{{ quote_column('plan') }}, cur.year_month
)

, provider_events as (
  select distinct
      me.person_id
    , me.data_source
    , me.payer
    , me.{{ quote_column('plan') }}
    , me.billing_id
    , me.encounter_start_date as event_date
  from mc_enriched me
  inner join subset_persons sp on me.person_id = sp.person_id
  where me.billing_id is not null
)

, facility_events as (
  select distinct
      me.person_id
    , me.data_source
    , me.payer
    , me.{{ quote_column('plan') }}
    , me.facility_id
    , me.encounter_start_date as event_date
  from mc_enriched me
  inner join subset_persons sp on me.person_id = sp.person_id
  where me.facility_id is not null
)

, provider_agg as (
  select
      idx.person_id
    , idx.data_source
    , idx.payer
    , idx.{{ quote_column('plan') }}
    , idx.index_year_month
    , count(distinct pe.billing_id) as lookback_distinct_providers_12m
  from idx
  inner join subset_persons sp on idx.person_id = sp.person_id
  left join provider_events pe
    on idx.person_id = pe.person_id
   and idx.data_source = pe.data_source
   and idx.payer = pe.payer
   and idx.{{ quote_column('plan') }} = pe.{{ quote_column('plan') }}
   and pe.event_date between idx.lookback_start_date and idx.index_month_end_date
  group by idx.person_id, idx.data_source, idx.payer, idx.{{ quote_column('plan') }}, idx.index_year_month
)

, facility_agg as (
  select
      idx.person_id
    , idx.data_source
    , idx.payer
    , idx.{{ quote_column('plan') }}
    , idx.index_year_month
    , count(distinct fe.facility_id) as lookback_distinct_facilities_12m
  from idx
  inner join subset_persons sp on idx.person_id = sp.person_id
  left join facility_events fe
    on idx.person_id = fe.person_id
   and idx.data_source = fe.data_source
   and idx.payer = fe.payer
   and idx.{{ quote_column('plan') }} = fe.{{ quote_column('plan') }}
   and fe.event_date between idx.lookback_start_date and idx.index_month_end_date
  group by idx.person_id, idx.data_source, idx.payer, idx.{{ quote_column('plan') }}, idx.index_year_month
)

select
    idx.person_id
  , idx.data_source
  , idx.payer
  , idx.{{ quote_column('plan') }}
  , idx.year_nbr
  , idx.diagnosis_year
  , idx.prediction_year
  , idx.index_year_month
  , idx.index_month
  , idx.index_month_end_date
  , idx.age_at_year_start
  , idx.sex
  , idx.race
  , idx.state
  , case when pc.person_id is null and pcms.person_id is null and phcc.person_id is null then 1 else 0 end as cold_start
{% for col in cond_cols %}
  {% set base = col | replace('cond_', '') %}
  , coalesce(pc.{{ base }}, 0) as lag_{{ col }}
{% endfor %}
{% for col in cms_cols %}
  , coalesce(pcms.{{ col }}, 0) as lag_{{ col }}
{% endfor %}
{% for col in hcc_cols %}
  , coalesce(phcc.{{ col }}, 0) as lag_{{ col }}
{% endfor %}
  , coalesce(lfm.target_12m_paid_amount, 0) as target_12m_paid_amount
  , cast(coalesce(lfm.target_12m_ed_encounter_count, 0) as {{ dbt.type_int() }}) as target_12m_ed_encounter_count
  , cast(coalesce(lfm.target_12m_acute_inpatient_encounter_count, 0) as {{ dbt.type_int() }}) as target_12m_acute_inpatient_encounter_count
  , cast(coalesce(lfm.lookback_ip_encounter_count_12m, 0) as {{ dbt.type_int() }}) as lookback_ip_encounter_count_12m
  , cast(coalesce(lfm.lookback_ed_encounter_count_12m, 0) as {{ dbt.type_int() }}) as lookback_ed_encounter_count_12m
  , cast(coalesce(lfm.lookback_ip_encounter_count_9m, 0) as {{ dbt.type_int() }}) as lookback_ip_encounter_count_9m
  , cast(coalesce(lfm.lookback_ed_encounter_count_9m, 0) as {{ dbt.type_int() }}) as lookback_ed_encounter_count_9m
  , cast(coalesce(lfm.lookback_ip_encounter_count_6m, 0) as {{ dbt.type_int() }}) as lookback_ip_encounter_count_6m
  , cast(coalesce(lfm.lookback_ed_encounter_count_6m, 0) as {{ dbt.type_int() }}) as lookback_ed_encounter_count_6m
  , cast(coalesce(lfm.lookback_ip_encounter_count_3m, 0) as {{ dbt.type_int() }}) as lookback_ip_encounter_count_3m
  , cast(coalesce(lfm.lookback_ed_encounter_count_3m, 0) as {{ dbt.type_int() }}) as lookback_ed_encounter_count_3m
  , coalesce(lfm.lookback_med_cost_12m, 0) as lookback_med_cost_12m
  , coalesce(lfm.lookback_med_ip_cost_12m, 0) as lookback_med_ip_cost_12m
  , coalesce(lfm.lookback_med_ed_cost_12m, 0) as lookback_med_ed_cost_12m
  , coalesce(lfm.lookback_rx_cost_12m, 0) as lookback_rx_cost_12m
  , coalesce(lfm.lookback_total_paid_12m, 0) as lookback_total_paid_12m
  , coalesce(lfm.lookback_total_paid_9m, 0) as lookback_total_paid_9m
  , coalesce(lfm.lookback_total_paid_6m, 0) as lookback_total_paid_6m
  , coalesce(lfm.lookback_total_paid_3m, 0) as lookback_total_paid_3m
  , cast(coalesce(lfm.lookback_highcost_month_count_12m, 0) as {{ dbt.type_int() }}) as lookback_highcost_month_count_12m
  , coalesce(lfm.lookback_stdev_cost_monthly_12m, 0) as lookback_stdev_cost_monthly_12m
  , cast(coalesce(pa.lookback_distinct_providers_12m, 0) as {{ dbt.type_int() }}) as lookback_distinct_providers_12m
  , cast(coalesce(fa.lookback_distinct_facilities_12m, 0) as {{ dbt.type_int() }}) as lookback_distinct_facilities_12m
  , case when lfm.last_ip_date_window is not null
         then {{ datediff('lfm.last_ip_date_window', 'idx.index_month_end_date', 'day') }}
         else null end as lookback_days_since_last_ip
  , case when lfm.last_ed_date_window is not null
         then {{ datediff('lfm.last_ed_date_window', 'idx.index_month_end_date', 'day') }}
         else null end as lookback_days_since_last_ed
  , case when lfm.last_highcost_month_end is not null
         then {{ datediff('lfm.last_highcost_month_end', 'idx.index_month_end_date', 'day') }}
         else null end as lookback_days_since_last_highcost
from idx
left join lookback_future_metrics lfm
  on idx.person_id = lfm.person_id
 and idx.data_source = lfm.data_source
 and idx.payer = lfm.payer
 and idx.{{ quote_column('plan') }} = lfm.{{ quote_column('plan') }}
 and idx.index_year_month = lfm.year_month
left join provider_agg pa
  on idx.person_id = pa.person_id
 and idx.data_source = pa.data_source
 and idx.payer = pa.payer
 and idx.{{ quote_column('plan') }} = pa.{{ quote_column('plan') }}
 and idx.index_year_month = pa.index_year_month
left join facility_agg fa
  on idx.person_id = fa.person_id
 and idx.data_source = fa.data_source
 and idx.payer = fa.payer
 and idx.{{ quote_column('plan') }} = fa.{{ quote_column('plan') }}
 and idx.index_year_month = fa.index_year_month
left join {{ ref('benchmarks__pivot_condition') }} pc
  on idx.person_id = pc.person_id
 and idx.diagnosis_year = pc.year_nbr
left join {{ ref('benchmarks__pivot_cms_condition') }} pcms
  on idx.person_id = pcms.person_id
 and idx.diagnosis_year = pcms.year_nbr
left join {{ ref('benchmarks__pivot_hcc') }} phcc
  on idx.person_id = phcc.person_id
 and idx.diagnosis_year = phcc.year_nbr
