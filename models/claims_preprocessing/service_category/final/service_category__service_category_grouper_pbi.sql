{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}


select
    claim_type
    ,claim_line_id
    , service_category_1
    , service_category_2
    , service_category_3
    , original_service_cat_2
    , original_service_cat_3
    , ccs_category
    , ccs_category_description
    , hcpcs_code
    , ms_drg_code
    , ms_drg_description
    , place_of_service_code
    , place_of_service_description
    , revenue_center_code
    , revenue_center_description
    , default_ccsr_category_ip
    , default_ccsr_category_op
    , default_ccsr_category_description_ip
    , default_ccsr_category_description_op
    , primary_taxonomy_code
    , primary_specialty_description
    , modality
    , bill_type_code
    , bill_type_description
    , duplicate_row_number
    , source_model_name
    , count(*) as claim_line_cnt
    , count(distinct claim_id) as claim_count
from {{ ref('service_category__service_category_grouper') }} g
group by 
    claim_type
    , service_category_1
    , service_category_2
    , service_category_3
    , original_service_cat_2
    , original_service_cat_3
    , ccs_category
    , ccs_category_description
    , hcpcs_code
    , ms_drg_code
    , ms_drg_description
    , place_of_service_code
    , place_of_service_description
    , revenue_center_code
    , revenue_center_description
    , default_ccsr_category_ip
    , default_ccsr_category_op
    , default_ccsr_category_description_ip
    , default_ccsr_category_description_op
    , primary_taxonomy_code
    , primary_specialty_description
    , modality
    , bill_type_code
    , bill_type_description
    , duplicate_row_number
    , source_model_name
    , claim_line_id
 
