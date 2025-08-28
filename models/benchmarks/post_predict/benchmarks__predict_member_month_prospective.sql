{#-- 1. Get all column objects from the upstream model --#}
{%- set columns = adapter.get_columns_in_relation(ref('benchmarks__predict_member_month')) -%}

{#-- 2. Create empty lists to store the column names we want --#}
{%- set amount_cols = [] -%}
{%- set count_cols = [] -%}

{#-- 3. Loop through all columns and sort them into the correct list --#}
{%- for col in columns -%}
    {%- set col_name = col.name | lower -%}
    {%- if 'actual' in col_name -%}
        {%- if col_name.endswith('amount') -%}
            {%- do amount_cols.append(col.name) -%}
        {%- elif col_name.endswith('count') -%}
            {%- do count_cols.append(col.name) -%}
        {%- endif -%}
    {%- endif -%}
{%- endfor -%}


select
   mm.benchmark_key
 ,mm.first_day_of_month
 ,mm.year_month
 ,mm.person_id
 ,mm.payer
 , mm.{{ quote_column('plan') }}
 ,mm.data_source
 ,pred.prediction_year
 ,mm.actual_paid_amount
 ,mm.actual_inpatient_paid_amount
 ,mm.actual_outpatient_paid_amount
 ,mm.actual_office_based_paid_amount
 ,mm.actual_other_paid_amount
 ,mm.actual_inpatient_encounter_count
 ,mm.actual_outpatient_encounter_count
 ,mm.actual_office_based_encounter_count
 ,mm.actual_other_encounter_count
 ,pred.pred_pmpm_outpatient as expected_outpatient_paid_amount
 ,pred.pred_pmpm_inpatient as expected_inpatient_paid_amount
 ,pred.pred_pmpm_other as expected_other_paid_amount
 ,pred.pred_pmpm_office_based as expected_office_based_paid_amount
 ,pred.pred_pmpc_outpatient as expected_outpatient_encounter_count
 ,pred.pred_pmpc_other as expected_other_encounter_count
 ,pred.pred_pmpc_office_based as expected_office_based_encounter_count
 ,pred.pred_pmpc_inpatient as expected_inpatient_encounter_count
from {{ ref('benchmarks__predict_member_month') }} mm
inner join {{ var('predictions_person_year_prospective') }} pred on pred.benchmark_key = mm.benchmark_key