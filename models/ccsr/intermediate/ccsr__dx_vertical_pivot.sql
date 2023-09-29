{{ config(
     enabled = var('ccsr_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with codes as (
    
    select
        icd_10_cm_code as code, 
        icd_10_cm_code_description as code_description,
        -- loop to generate columns for CCSR categories 1-6
        {%- for i in range(1,7) %}
        ccsr_category_{{ i }},
        ccsr_category_{{ i }}_description,
        {%- endfor %}
        default_ccsr_category_ip,
        default_ccsr_category_op
    from {{ ref('ccsr__dxccsr_v2023_1_cleaned_map') }}

), long_union as (
    -- generate select & union statements to pivot category columns to rows
    {% for i in range(1,7,1) %}
    select 
        code,
        code_description,
        left(ccsr_category_{{ i }}, 3) as ccsr_parent_category,
        ccsr_category_{{ i }} as ccsr_category,
        ccsr_category_{{ i }}_description as ccsr_category_description,
        {{ i }} as ccsr_category_rank,
        (ccsr_category_{{ i }} = default_ccsr_category_ip) as is_ip_default_category,
        (ccsr_category_{{ i }} = default_ccsr_category_op) as is_op_default_category
    from codes 
    {{ "union all" if not loop.last else "" }}
    {%- endfor %}

)

select distinct
    *,
    '{{ var('tuva_last_run')}}' as tuva_last_run
from long_union
-- as not all diagnosis codes have multiple categories, we can discard nulls
where ccsr_category is not null