{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

{# ---To avoid listing each feature and target dynamically, based on person year table --- #}
{% set src = ref('benchmarks__person_year') %}
{% set cols = adapter.get_columns_in_relation(src) %}

{% set paid_cols = [] %}
{% set count_cols = [] %}
{% set cond_cols = [] %}
{% set cms_cols  = [] %}
{% set hcc_cols  = [] %}

{# classify columns by name #}
{% for c in cols %}
  {% set n = c.name | lower %}
  {% if n.endswith('_paid_amount') %}
    {% do paid_cols.append(n) %}
  {% elif n.endswith('_count') %}
    {% do count_cols.append(n) %}
  {% elif n.startswith('cond_') %}
    {% do cond_cols.append(n) %}
  {% elif n.startswith('cms_') %}
    {% do cms_cols.append(n) %}
  {% elif n.startswith('hcc_') %}
    {% do hcc_cols.append(n) %}
  {% endif %}
{% endfor %}

select
  py.benchmark_key
, py.person_id
, py.payer
, py.{{ quote_column('plan') }}
, py.data_source
, py.year_nbr                               as prediction_year
, (py.year_nbr - 1)                         as diagnosis_year

, py.paid_amount                            as prediction_year_paid_amount
, py.member_month_count                     as prediction_year_member_months

, py.age_at_year_start                      as prediction_year_age_at_year_start
, py.sex                                    as prediction_year_sex
, py.race                                   as prediction_year_race
, py.state                                  as prediction_year_state

-- if person not in any condition table, create cold_start indicator
, case when pc.person_id is null and pcms.person_id is null and phcc.person_id is null then 1 else 0 end as cold_start

{# --- prediction-year PMPM/PMPC --- #}
, case when coalesce(py.member_month_count,0)=0
       then 0
       else coalesce(py.paid_amount,0)/py.member_month_count
  end                                          as prediction_year_pmpm_paid_amount

{% for col in paid_cols %}
  {% if col != 'paid_amount' %}
, case when coalesce(py.member_month_count,0)=0
       then 0
       else coalesce(py.{{ col }}, 0)/py.member_month_count
  end                                          as prediction_year_pmpm_{{ col }}
  {% endif %}
{% endfor %}

{% for col in count_cols %}
  {% if col != 'member_month_count' %}
, case when coalesce(py.member_month_count,0)=0
       then 0
       else coalesce(py.{{ col }}, 0)/py.member_month_count
  end                                          as prediction_year_pmpc_{{ col }}
  {% endif %}
{% endfor %}


{# --- lag condition flags from pivot_condition (prior year) --- #}
{% for col in cond_cols %}
  {% set base = col | replace('cond_', '') %}
, coalesce(pc.{{ base }}, 0)                 as lag_{{ col }}
{% endfor %}

{# --- lag cms flags from pivot_cms_condition (prior year) --- #}
{% for col in cms_cols %}
, coalesce(pcms.{{ col }}, 0)                as lag_{{ col }}
{% endfor %}

{# --- lag hcc flags from pivot_hcc (prior year) --- #}
{% for col in hcc_cols %}
, coalesce(phcc.{{ col }}, 0)                as lag_{{ col }}
{% endfor %}



from {{ ref('benchmarks__person_year') }} py
left join {{ ref('benchmarks__pivot_condition') }} as pc
  on py.person_id = pc.person_id and (py.year_nbr - 1) = pc.year_nbr
left join {{ ref('benchmarks__pivot_cms_condition') }} as pcms
  on py.person_id = pcms.person_id and (py.year_nbr - 1) = pcms.year_nbr
left join {{ ref('benchmarks__pivot_hcc') }} as phcc
  on py.person_id = phcc.person_id and (py.year_nbr - 1) = phcc.year_nbr
