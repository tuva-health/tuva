{{ config(
     enabled = var('ccsr_enabled',var('tuva_marts_enabled',True))
   )
}}

{% set categories_list = dbt_utils.get_column_values(
        table=ref("ccsr__procedure_category_map"),
        column="ccsr_category",
        order_by="ccsr_category"
) %}

with dedupe_records as (

    select distinct
        encounter_id,
        patient_id,
        ccsr_category
    from {{ ref('ccsr__long_procedure_category') }} 

)

select
    encounter_id,
    patient_id,
    -- pivot rows into column values for each possible CCSR category
    {% for category in categories_list %}
    -- as we don't rank procedure codes, we encode to 0 or 1 instead of 0-3
    sum(case when ccsr_category = '{{ category }}' then 1 else 0 end) as prccsr_{{ category|lower }},
    {% endfor %}
    {{ var('prccsr_version') }} as prccsr_version,
    '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as _model_run_time
from dedupe_records
group by encounter_id, patient_id, prccsr_version, _model_run_time