{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


with pivot_procedure as(
    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_1'  as procedure_column
        ,  procedure_code_1  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_2'  as procedure_column
        ,  procedure_code_2  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_3'  as procedure_column
        ,  procedure_code_3  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_4'  as procedure_column
        ,  procedure_code_4  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_5'  as procedure_column
        ,  procedure_code_5  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_6'  as procedure_column
        ,  procedure_code_6  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_7'  as procedure_column
        ,  procedure_code_7  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_8'  as procedure_column
        ,  procedure_code_8  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_9'  as procedure_column
        ,  procedure_code_9  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_10'  as procedure_column
        ,  procedure_code_10  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_11'  as procedure_column
        ,  procedure_code_11  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_12'  as procedure_column
        ,  procedure_code_12  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_13'  as procedure_column
        ,  procedure_code_13  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_14'  as procedure_column
        ,  procedure_code_14  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_15'  as procedure_column
        ,  procedure_code_15  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_16'  as procedure_column
        ,  procedure_code_16  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_17'  as procedure_column
        ,  procedure_code_17  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_18'  as procedure_column
        ,  procedure_code_18  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_19'  as procedure_column
        ,  procedure_code_19  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_20'  as procedure_column
        ,  procedure_code_20  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_21'  as procedure_column
        ,  procedure_code_21  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_22'  as procedure_column
        ,  procedure_code_22  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_23'  as procedure_column
        ,  procedure_code_24  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_24'  as procedure_column
        ,  procedure_code_24  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , procedure_code_type
        , 'procedure_code_25'  as procedure_column
        ,  procedure_code_25  as procedure_code
    from {{ ref('normalized_input__stg_medical_claim') }}
)

select
    claim_id
    , data_source
    , procedure_code_type
    , procedure_column
    , coalesce(icd_9.icd_9_pcs,icd_10.icd_10_pcs) as normalized_procedure_code
    , count(*) as procedure_code_occurrence_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from pivot_procedure piv
left join {{ ref('terminology__icd_10_pcs') }} icd_10
    on replace(piv.procedure_code,'.','') = icd_10.icd_10_pcs
    and piv.procedure_code_type = 'icd-10-pcs'
left join {{ ref('terminology__icd_9_pcs') }} icd_9
    on replace(piv.procedure_code,'.','') = icd_9.icd_9_pcs
    and piv.procedure_code_type = 'icd-9-pcs'
where claim_type = 'institutional'
group by 
    claim_id
    , data_source
    , procedure_code_type
    , procedure_column
    , coalesce(icd_9.icd_9_pcs,icd_10.icd_10_pcs)