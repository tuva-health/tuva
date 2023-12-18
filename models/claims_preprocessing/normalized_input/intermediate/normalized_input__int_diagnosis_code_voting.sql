{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with distinct_count as(
    select
        claim_id
        , data_source
        , diagnosis_column
        , count(*) as distinct_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('normalized_input__int_diagnosis_code_normalize') }}
    group by
        claim_id
        , data_source
        , diagnosis_column
)

select 
    norm.claim_id
    , norm.data_source
    , norm.diagnosis_column as column_name
    , norm.normalized_diagnosis_code as normalized_code
    , norm.diagnosis_code_occurrence_count as occurrence_count
    , coalesce(lead(diagnosis_code_occurrence_count) 
        over (partition by norm.claim_id, norm.data_source, norm.diagnosis_column order by diagnosis_code_occurrence_count desc),0) as next_occurrence_count
    , row_number() over (partition by norm.claim_id, norm.data_source, norm.diagnosis_column order by diagnosis_code_occurrence_count desc) as occurrence_row_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__int_diagnosis_code_normalize') }} norm
inner join distinct_count dist
    on norm.claim_id = dist.claim_id
    and norm.data_source = dist.data_source
    and norm.diagnosis_column = dist.diagnosis_column
