{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

{% set procedure_cols = range(1, 26) %}

select
    claim_id
    , data_source
    {% for i in procedure_cols %}
    , max(case when column_name = 'procedure_code_{{ i }}' then procedure_code else null end) as procedure_code_{{ i }}
    {% endfor %}
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('claims_normalization__int_procedure_code_intermediate') }}
group by
    claim_id
    , data_source
