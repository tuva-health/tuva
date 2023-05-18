{{ config(
     enabled = var('ccsr_enabled',var('tuva_marts_enabled',True))
   )
}}

{% set categories_list = dbt_utils.get_column_values(
        table=ref("ccsr__dx_vertical_pivot"),
        column="ccsr_category",
        order_by="ccsr_category"
) %}


with bool_ranks as (

    -- bool agg functions will reduce the long table to one row per CCSR category per encounter
    select 
        encounter_id,
        claim_id,
        patient_id,
        ccsr_category,
        ccsr_category like 'XXX%' as is_excluded,
        booland_agg(diagnosis_rank = 1) as is_only_first,
        boolor_agg(diagnosis_rank = 1) as is_first,
        boolor_agg(diagnosis_rank >= 1) as is_nth,
        boolor_agg(diagnosis_rank > 1) as not_first
    from {{ ref('ccsr__long_condition_category') }}
    {{ dbt_utils.group_by(n=5) }}

), bool_logic as (

    select distinct
        encounter_id,
        claim_id,
        patient_id,
        ccsr_category,
        -- assigns one of four values for each DXCCSR data element as per pg 25 of DXCCSR User guide v2023.1
        case 
            when not is_nth then 0
            when is_only_first and not is_excluded then 1
            when is_first and is_nth and not is_excluded then 2
            when not_first then 3 
            else -99 
            end as dx_code
    from bool_ranks 

)

select distinct
    encounter_id,
    claim_id,
    patient_id,
    -- pivot rows into column values for each possible CCSR category
    {% for category in categories_list %}
    sum(case when ccsr_category = '{{ category }}' then dx_code else 0 end) as dxccsr_{{ category }},
    {% endfor %}
    {{ var('dxccsr_version') }} as dxccsr_version,
    '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as _model_run_time
from bool_logic
group by encounter_id, claim_id, patient_id, dxccsr_version, _model_run_time