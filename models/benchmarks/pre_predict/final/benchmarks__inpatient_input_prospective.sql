{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

{% set cols = adapter.get_columns_in_relation( ref('benchmarks__inpatient_input') ) %}

{# collect column names (lowercased) #}
{% set col_names = [] %}
{% for c in cols %}
  {% do col_names.append(c.name | lower) %}
{% endfor %}

{# identify condition groups dynamically #}
{% set cond_cols = [] %}
{% set cms_cols  = [] %}
{% set hcc_cols  = [] %}
{% for n in col_names %}
  {% if n.startswith('cond_') %}
    {% do cond_cols.append(n) %}
  {% elif n.startswith('cms_') %}
    {% do cms_cols.append(n) %}
  {% elif n.startswith('hcc_') %}
    {% do hcc_cols.append(n) %}
  {% endif %}
{% endfor %}

with ip as (
  select b.*
  ,e.person_id
  from {{ ref('benchmarks__inpatient_input') }} b
  left join {{ ref('core__encounter') }} e on b.encounter_id = e.encounter_id
),

/* Demographics per person-year (aggregate even if upstream is stable) */
demos_py as (
  select
      person_id
    , data_source
    , year_nbr
    , min(sex)                 as lag_sex
    , min(race)                as lag_race
    , min(state)               as lag_state
    , avg(age_at_admit)        as lag_age_at_admit
  from ip
  group by person_id
    , data_source
    , year_nbr
),

/* Condition flags per person-year (binary → MAX) */
conds_py as (
  select
      person_id
    , data_source
    , year_nbr
    {% for col in cond_cols %}
    , max({{ col }})           as lag_{{ col }}
    {% endfor %}
    {% for col in cms_cols %}
    , max({{ col }})           as lag_{{ col }}
    {% endfor %}
    {% for col in hcc_cols %}
    , max({{ col }})           as lag_{{ col }}
    {% endfor %}
  from ip
  group by person_id
    , data_source
    , year_nbr
)

select
    pred.encounter_id
  , pred.person_id
  , pred.data_source
  , pred.year_nbr                                      as prediction_year
  , pred.year_nbr - 1                                  as diagnosis_year

  -- prediction-year demographics from encounter
  , pred.sex                                           as prediction_year_sex
  , pred.race                                          as prediction_year_race
  , pred.state                                         as prediction_year_state
  , pred.age_at_admit                                  as prediction_year_age_at_admit

  -- encounter context / potential targets
  , pred.length_of_stay
  , pred.discharge_location
  , pred.ms_drg_code
  , pred.ccsr_cat
  , pred.readmission_numerator
  , pred.readmission_denominator

  -- lag presence flags
  , case when d.person_id is null or c.person_id is null then 1 else 0 end as lag_missing
  , case when d.person_id is null and c.person_id is null then 1 else 0 end as cold_start

  -- lagged demographics (prior person-year)
  , d.lag_sex
  , d.lag_race
  , d.lag_state
  , d.lag_age_at_admit

  -- lagged conditions (prior person-year)
  {% for col in cond_cols %}
  , coalesce(c.lag_{{ col }}, 0)                       as lag_{{ col }}
  {% endfor %}
  {% for col in cms_cols %}
  , coalesce(c.lag_{{ col }}, 0)                       as lag_{{ col }}
  {% endfor %}
  {% for col in hcc_cols %}
  , coalesce(c.lag_{{ col }}, 0)                       as lag_{{ col }}
  {% endfor %}

from ip as pred
left join demos_py d
  on pred.person_id    = d.person_id
 and pred.data_source  = d.data_source
 and pred.year_nbr - 1 = d.year_nbr
left join conds_py c
  on pred.person_id    = c.person_id
 and pred.data_source  = c.data_source
 and pred.year_nbr - 1 = c.year_nbr
