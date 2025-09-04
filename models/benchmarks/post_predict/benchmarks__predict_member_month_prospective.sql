{{
    config(
        enabled = var('benchmarks_already_created', false) | as_bool
    )
}}

{# Get columns from the non-prospective monthly table to mirror naming #}
{%- set columns = adapter.get_columns_in_relation(ref('benchmarks__predict_member_month')) -%}

{%- set actual_cols = [] -%}
{%- for col in columns -%}
  {%- set n = col.name | lower -%}
  {%- if n.startswith('actual_') -%}
    {%- do actual_cols.append(col.name) -%}
  {%- endif -%}
{%- endfor -%}

{# Build dynamic lists from actual_* columns for both groups and types #}
{%- set paid_pairs = [] -%}  {# list of [pred_key, output_alias] #}
{%- set count_pairs = [] -%} {# list of [pred_key, output_alias] #}
{%- set pred_exclude = [] -%}

{%- for c in actual_cols -%}
  {%- set n = c | lower -%}
  {%- if n.endswith('_paid_amount') -%}
    {%- set base = n | replace('actual_','') | replace('_paid_amount','') -%}
    {%- set pred_key = base if base != '' else 'overall' -%}
    {%- set out_alias = 'expected_paid_amount' if base == '' else 'expected_' ~ base ~ '_paid_amount' -%}
    {%- if pred_key not in pred_exclude -%}
      {%- do paid_pairs.append([pred_key, out_alias]) -%}
    {%- endif -%}
  {%- elif n.endswith('_encounter_count') -%}
    {%- set base = n | replace('actual_','') | replace('_encounter_count','') -%}
    {%- set pred_key = base -%}
    {%- set out_alias = 'expected_' ~ base ~ '_encounter_count' -%}
    {%- if pred_key not in pred_exclude -%}
      {%- do count_pairs.append([pred_key, out_alias]) -%}
    {%- endif -%}
  {%- endif -%}
{%- endfor -%}

select
   mm.benchmark_key
 , mm.first_day_of_month
 , mm.year_month
 , mm.person_id
 , mm.payer
 , mm.{{ quote_column('plan') }}
 , mm.data_source
 , cast(left(mm.year_month, 4) as {{ dbt.type_int() }}) as prediction_year

 {# Bring through all actual_* columns dynamically from mm #}
{% for col in actual_cols %}
 , mm.{{ col }}
{% endfor %}

 {# Expected paid amounts from prospective predictions (PMPM), groups + types #}
{% for pred_key, out_col in paid_pairs %}
 , pred.pred_pmpm_{{ pred_key }} as {{ out_col }}
{% endfor %}

 {# Expected encounter counts from prospective predictions (PMPC), groups + types #}
{% for pred_key, out_col in count_pairs %}
 , pred.pred_pmpc_{{ pred_key }} as {{ out_col }}
{% endfor %}

from {{ ref('benchmarks__predict_member_month') }} as mm
inner join {{ var('predictions_person_year_prospective') }} as pred
  on pred.benchmark_key = mm.benchmark_key
