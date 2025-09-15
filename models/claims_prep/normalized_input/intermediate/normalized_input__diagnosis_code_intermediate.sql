-- Define the range of diagnosis columns to pivot (e.g., diagnosis_code_1 to diagnosis_code_25)
{% set diagnosis_cols = range(1, 26) %}

-- Pivot diagnosis columns into a long format
with pivot_diagnosis as (
    {% for i in diagnosis_cols %}
    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , cast({{ i }} as {{ dbt.type_int() }}) as diagnosis_position
        , 'diagnosis_code_{{ i }}' as column_name
        , diagnosis_code_{{ i }} as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
    where diagnosis_code_{{ i }} is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

-- Final output: distinct pivoted diagnosis records
select distinct
    claim_id
    , data_source
    , diagnosis_code_type
    , diagnosis_position
    , column_name
    , replace(diagnosis_code, '.', '') as diagnosis_code
from pivot_diagnosis

select
    claim_id
    , data_source
    {% for i in diagnosis_cols %}
    , max(case when column_name = 'diagnosis_code_{{ i }}' then diagnosis_code else null end) as diagnosis_code_{{ i }}
    {% endfor %}
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('normalized_input__int_diagnosis_code_intermediate') }}
group by
    claim_id
    , data_source