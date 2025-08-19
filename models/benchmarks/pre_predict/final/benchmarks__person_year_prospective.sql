{# --- build dynamic column groups from the source relation --- #}
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
  py.person_id
, py.payer
, py.plan
, py.data_source
, py.year_nbr                               as prediction_year
, py_diag.year_nbr                          as diagnosis_year

, py.paid_amount                            as person_year_paid_amount
, py.member_month_count                     as person_year_member_months

, case when py_diag.person_id is null then 1 else 0 end as lag_missing
, coalesce(py_diag.member_month_count, 0)   as lag_member_months

, coalesce(py_diag.paid_amount, 0)          as diagnosis_year_paid_amount
, case when coalesce(py_diag.member_month_count,0)=0
       then 0
       else coalesce(py_diag.paid_amount,0)/py_diag.member_month_count
  end                                       as lag_pmpm_paid_amount

, py.age_at_year_start                      as age_t
, py.sex                                    as sex_t
, py.race                                   as race_t
, py.state                                  as state_t

{# --- lag raw paid amounts and their PMPM normalizations --- #}
{% for col in paid_cols %}
, coalesce(py_diag.{{ col }}, 0)            as lag_{{ col }}
, case when coalesce(py_diag.member_month_count,0)=0
       then 0
       else coalesce(py_diag.{{ col }}, 0)/py_diag.member_month_count
  end                                       as lag_pmpm_{{ col }}
{% endfor %}

{# --- lag raw counts and their PMPC normalizations --- #}
{% for col in count_cols %}
, coalesce(py_diag.{{ col }}, 0)            as lag_{{ col }}
, case when coalesce(py_diag.member_month_count,0)=0
       then 0
       else coalesce(py_diag.{{ col }}, 0)/py_diag.member_month_count
  end                                       as lag_pmpc_{{ col }}
{% endfor %}

{# --- lag condition flags --- #}
{% for col in cond_cols %}
, coalesce(py_diag.{{ col }}, 0)            as lag_{{ col }}
{% endfor %}

{# --- lag cms flags --- #}
{% for col in cms_cols %}
, coalesce(py_diag.{{ col }}, 0)            as lag_{{ col }}
{% endfor %}

{# --- lag hcc flags --- #}
{% for col in hcc_cols %}
, coalesce(py_diag.{{ col }}, 0)            as lag_{{ col }}
{% endfor %}

from {{ ref('benchmarks__person_year') }} py
left join {{ ref('benchmarks__person_year') }} py_diag
  on py.person_id   = py_diag.person_id
 and py.payer       = py_diag.payer
 and py.plan        = py_diag.plan
 and py.data_source = py_diag.data_source
 and py.year_nbr - 1 = py_diag.year_nbr
