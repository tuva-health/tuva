{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

{% set diagnosis_cols = range(1, 26) %}

select
    claim_id
    , data_source
    , max(raw_code_system) as diagnosis_code_type
    {% for i in diagnosis_cols %}
    , max(case when condition_rank = {{ i }} then source_code else null end) as diagnosis_code_{{ i }}
    {% endfor %}
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('normalized__medical_claim_diagnosis') }}
group by
    claim_id
    , data_source
