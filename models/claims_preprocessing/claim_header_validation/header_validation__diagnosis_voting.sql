{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set header_column_list = [
     'diagnosis_code_1_normalized'
    , 'diagnosis_code_2_normalized'
    , 'diagnosis_code_3_normalized'
] -%}


with header_occurrences as(

 {{ header_validation_diagnosis_voting(ref('header_validation__diagnosis_normalize'),ref('header_validation__diagnosis_unique_count'), header_column_list) }}

)

select
    claim_id
    , data_source
    , column_checked
    , diagnosis_normalized
    , occurrence_count
    , next_occurrence_count
    , occurrence_row_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from header_occurrences