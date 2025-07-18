{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False)))
     | as_bool
   )
}}

{% set diagnosis_cols = range(1, 26) %}

with pivot_diagnosis as (
    {% for i in diagnosis_cols %}
    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_{{ i }}' as column_name
        , diagnosis_code_{{ i }} as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
    where diagnosis_code_{{ i }} is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select distinct
    claim_id
    , data_source
    , diagnosis_code_type
    , column_name
    , replace(piv.diagnosis_code, '.', '') as diagnosis_code
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from pivot_diagnosis as piv
where claim_type <> 'undetermined'
