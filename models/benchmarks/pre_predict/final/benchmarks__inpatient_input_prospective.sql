{# {{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}} #}

/*
  Prospective inpatient input:
  - Prediction-year encounter context at encounter grain (same as non-prospective input)
  - Lagged feature flags joined directly from person-year pivot tables with year_nbr - 1
*/

{# --- Build dynamic lists of lag feature columns from pivot tables --- #}
{% set rel_pc   = ref('benchmarks__pivot_condition') %}
{% set rel_pcms = ref('benchmarks__pivot_cms_condition') %}
{% set rel_phcc = ref('benchmarks__pivot_hcc') %}

{% set pc_cols   = adapter.get_columns_in_relation(rel_pc) %}
{% set pcms_cols = adapter.get_columns_in_relation(rel_pcms) %}
{% set phcc_cols = adapter.get_columns_in_relation(rel_phcc) %}

{% set excluded_pc_cols = ['person_id','year_nbr','payer','plan','data_source','benchmark_key','tuva_last_run'] %}

{% set cond_cols = [] %}
{% for c in pc_cols %}
  {% set n = c.name | lower %}
  {% if n not in excluded_pc_cols %}
    {% do cond_cols.append(n) %}
  {% endif %}
{% endfor %}

{% set cms_cols = [] %}
{% for c in pcms_cols %}
  {% set n = c.name | lower %}
  {% if n.startswith('cms_') %}
    {% do cms_cols.append(n) %}
  {% endif %}
{% endfor %}

{% set hcc_cols = [] %}
{% for c in phcc_cols %}
  {% set n = c.name | lower %}
  {% if n.startswith('hcc_') %}
    {% do hcc_cols.append(n) %}
  {% endif %}
{% endfor %}

with base as (
  select
      ii.encounter_id
    , ce.person_id
    , ii.data_source
    , ii.year_nbr as prediction_year
    , ii.year_nbr - 1 as diagnosis_year
    , ii.sex as prediction_year_sex
    , ii.race as prediction_year_race
    , ii.state as prediction_year_state
    , ii.age_at_admit as prediction_year_age_at_admit
    , ii.length_of_stay
    , ii.discharge_location
    , ii.ms_drg_code
    , ii.ccsr_cat
    , ii.readmission_numerator
    , ii.readmission_denominator
  from {{ ref('benchmarks__inpatient_input') }} as ii
  inner join {{ ref('benchmarks__stg_core__encounter') }} as ce
    on ii.encounter_id = ce.encounter_id
)

select
    b.encounter_id
  , b.person_id
  , b.data_source
  , b.prediction_year
  , b.diagnosis_year
  , b.prediction_year_sex
  , b.prediction_year_race
  , b.prediction_year_state
  , b.prediction_year_age_at_admit
  , b.length_of_stay
  , b.discharge_location
  , b.ms_drg_code
  , b.ccsr_cat
  , b.readmission_numerator
  , b.readmission_denominator

  -- lag presence flags (based on availability of any prior-year pivot rows)
  , case when pc.person_id is null or pcms.person_id is null or phcc.person_id is null then 1 else 0 end as lag_missing
  , case when pc.person_id is null and pcms.person_id is null and phcc.person_id is null then 1 else 0 end as cold_start

  {# --- lagged Tuva chronic conditions (prior year) --- #}
{% for col in cond_cols %}
  , coalesce(pc.{{ col }}, 0) as lag_cond_{{ col }}
{% endfor %}

  {# --- lagged CMS condition flags (prior year) --- #}
{% for col in cms_cols %}
  , coalesce(pcms.{{ col }}, 0) as lag_{{ col }}
{% endfor %}

  {# --- lagged HCC flags (prior year) --- #}
{% for col in hcc_cols %}
  , coalesce(phcc.{{ col }}, 0) as lag_{{ col }}
{% endfor %}

  -- run metadata stamp
  , '{{ var('tuva_last_run') }}' as tuva_last_run

from base as b
left outer join {{ ref('benchmarks__pivot_condition') }} as pc
  on b.person_id = pc.person_id and b.diagnosis_year = pc.year_nbr
left outer join {{ ref('benchmarks__pivot_cms_condition') }} as pcms
  on b.person_id = pcms.person_id and b.diagnosis_year = pcms.year_nbr
left outer join {{ ref('benchmarks__pivot_hcc') }} as phcc
  on b.person_id = phcc.person_id and b.diagnosis_year = phcc.year_nbr
