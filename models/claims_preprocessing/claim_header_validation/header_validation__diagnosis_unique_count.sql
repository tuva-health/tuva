{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set header_column_list = [
     'diagnosis_code_1_normalized'
    , 'diagnosis_code_2_normalized'
    , 'diagnosis_code_3_normalized'
    , 'diagnosis_code_4_normalized'
    , 'diagnosis_code_5_normalized'
] -%}


with header_duplicates as(

 {{ header_validation_diagnosis_unique_count(ref('header_validation__diagnosis_normalize'), header_column_list) }}

)

select
    claim_id
    , data_source
    , column_checked
    , distinct_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from header_duplicates

