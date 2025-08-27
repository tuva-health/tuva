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
   pmm.year_month
 , pmm.first_day_of_month
 , pmm.person_id
 , pmm.payer
 , pmm.{{ quote_column('plan') }}
 , pmm.data_source
 , pmm.benchmark_key

 ,
    -- ==== ACTUAL paid amounts ====
    {%- for col in amount_cols %}
    {{ col }}{% if not loop.last %}, {% endif %}
    {%- endfor %}

 ,
    -- ==== ACTUAL encounter counts ====
    {%- for col in count_cols %}
    {{ col }}{% if not loop.last %}, {% endif %}
    {%- endfor %}

  --pmpm predictions
    ,pred_pmpm_overall
    ,pred_pmpm_outpatient
    ,pred_pmpm_inpatient
    ,pred_pmpm_other
    ,pred_pmpm_office_based

--pmpc predictions
    ,pred_pmpc_outpatient
    ,pred_pmpc_other
    ,pred_pmpc_office_based
    ,pred_pmpc_inpatient
from {{ ref('benchmarks__predict_member_month') }} pmm
inner join {{ var('predictions_person_year_prospective') }} pyp on pmm.benchmark_key = pyp.benchmark_key