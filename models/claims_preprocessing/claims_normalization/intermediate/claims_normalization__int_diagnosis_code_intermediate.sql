{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False)))
     | as_bool
   )
}}

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
        , 'diagnosis_code_{{ i }}' as column_name
        , diagnosis_code_{{ i }} as diagnosis_code
    from {{ ref('claims_normalization__stg_medical_claim') }}
    where diagnosis_code_{{ i }} is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

-- Final output: distinct pivoted diagnosis records
select distinct
    claim_id
    , data_source
    , diagnosis_code_type
    , column_name
    , replace(piv.diagnosis_code, '.', '') as diagnosis_code
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from pivot_diagnosis as piv
where claim_type <> 'undetermined'
