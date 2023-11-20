{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select 
    norm.claim_id
    , norm.data_source
    , norm.diagnosis_column
    , norm.normalized_diagnosis_code
    , norm.diagnosis_code_occurrence_count
    , coalesce(lead(diagnosis_code_occurrence_count) 
        over (partition by norm.claim_id, norm.data_source, norm.diagnosis_column order by diagnosis_code_occurrence_count desc),0) as next_occurrence_count
    , row_number() over (partition by norm.claim_id, norm.data_source, norm.diagnosis_column order by diagnosis_code_occurrence_count desc) as occurrence_row_count
from {{ ref('header_validation__int_diagnosis_normalize') }} norm
inner join {{ ref('header_validation__int_diagnosis_unique_count') }} uni
    on norm.claim_id = uni.claim_id
    and norm.data_source = uni.data_source
    and norm.diagnosis_column = uni.diagnosis_column
