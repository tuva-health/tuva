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

{# Build dynamic expected pairs directly from the predictions relation columns #}
{%- set pred_str = var('predictions_person_year_prospective') -%}
{%- set clean = pred_str | replace('`','') | replace('[','') | replace(']','') | replace('"','') -%}
{%- set parts = clean.split('.') -%}
{%- if parts | length == 3 -%}
  {%- set pred_rel = adapter.get_relation(database=parts[0], schema=parts[1], identifier=parts[2]) -%}
{%- elif parts | length == 2 -%}
  {%- set pred_rel = adapter.get_relation(database=target.database, schema=parts[0], identifier=parts[1]) -%}
{%- else -%}
  {%- set pred_rel = none -%}
{%- endif -%}

{%- set pred_cols = adapter.get_columns_in_relation(pred_rel) if pred_rel else [] -%}
{%- set paid_pairs = [] -%}  {# list of [base, output_alias] #}
{%- set count_pairs = [] -%} {# list of [base, output_alias] #}

{%- for col in pred_cols -%}
  {%- set n = col.name | lower -%}
  {%- if n.startswith('pred_pmpm_') -%}
    {%- set base = n | replace('pred_pmpm_','') -%}
    {%- set out_alias = 'expected_paid_amount' if base == 'overall' else 'expected_' ~ base ~ '_paid_amount' -%}
    {%- do paid_pairs.append([base, out_alias]) -%}
  {%- elif n.startswith('pred_pmpc_') -%}
    {%- set base = n | replace('pred_pmpc_','') -%}
    {%- set out_alias = 'expected_' ~ base ~ '_encounter_count' -%}
    {%- do count_pairs.append([base, out_alias]) -%}
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
{% for base, out_col in paid_pairs %}
 , pred.pred_pmpm_{{ base }} as {{ out_col }}
{% endfor %}

 {# Expected encounter counts from prospective predictions (PMPC), groups + types #}
{% for base, out_col in count_pairs %}
 , pred.pred_pmpc_{{ base }} as {{ out_col }}
{% endfor %}

from {{ ref('benchmarks__predict_member_month') }} as mm
inner join {{ var('predictions_person_year_prospective') }} as pred
  on pred.benchmark_key = mm.benchmark_key
