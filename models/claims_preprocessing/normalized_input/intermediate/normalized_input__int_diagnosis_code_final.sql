{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False)))
     | as_bool
   )
}}

{% set diagnosis_cols = range(1, 26) %}

select
    claim_id
    , data_source
    {% for i in diagnosis_cols %}
    , max(case when column_name = 'diagnosis_code_{{ i }}' then diagnosis_code else null end) as diagnosis_code_{{ i }}
    {% endfor %}
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('normalized_input__int_diagnosis_code_intermediate') }}
{# from {{ ref('normalized_input__int_diagnosis_code_voting') }} #}
group by
    claim_id
    , data_source
