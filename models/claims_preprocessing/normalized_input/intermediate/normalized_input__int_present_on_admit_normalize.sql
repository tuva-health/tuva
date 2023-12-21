{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


with pivot_poa as(
    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_1'  as diagnosis_column
        ,  diagnosis_poa_1  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_2'  as diagnosis_column
        ,  diagnosis_poa_2  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_3'  as diagnosis_column
        ,  diagnosis_poa_3  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_4'  as diagnosis_column
        ,  diagnosis_poa_4  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_5'  as diagnosis_column
        ,  diagnosis_poa_5  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_6'  as diagnosis_column
        ,  diagnosis_poa_6  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_7'  as diagnosis_column
        ,  diagnosis_poa_7  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_8'  as diagnosis_column
        ,  diagnosis_poa_8  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_9'  as diagnosis_column
        ,  diagnosis_poa_9  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_10'  as diagnosis_column
        ,  diagnosis_poa_10  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_11'  as diagnosis_column
        ,  diagnosis_poa_11  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_12'  as diagnosis_column
        ,  diagnosis_poa_12  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_13'  as diagnosis_column
        ,  diagnosis_poa_13  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_14'  as diagnosis_column
        ,  diagnosis_poa_14  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_15'  as diagnosis_column
        ,  diagnosis_poa_15  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_16'  as diagnosis_column
        ,  diagnosis_poa_16  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_17'  as diagnosis_column
        ,  diagnosis_poa_17  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_18'  as diagnosis_column
        ,  diagnosis_poa_18  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_19'  as diagnosis_column
        ,  diagnosis_poa_19  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_20'  as diagnosis_column
        ,  diagnosis_poa_20  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_21'  as diagnosis_column
        ,  diagnosis_poa_21  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_22'  as diagnosis_column
        ,  diagnosis_poa_22  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}


    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_23'  as diagnosis_column
        ,  diagnosis_poa_24  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_24'  as diagnosis_column
        ,  diagnosis_poa_24  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}

    union all

    select
        claim_id
        , claim_type
        , data_source
        , 'diagnosis_poa_25'  as diagnosis_column
        ,  diagnosis_poa_25  as present_on_admit_code
    from {{ ref('normalized_input__stg_medical_claim') }}
)

select
    claim_id
    , data_source
    , diagnosis_column
    , poa.present_on_admit_code as normalized_present_on_admit_code
    , count(*) as present_on_admit_occurrence_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from pivot_poa piv
left join {{ ref('terminology__present_on_admission') }} poa
    on replace(piv.present_on_admit_code,'.','') = poa.present_on_admit_code
where claim_type = 'institutional'
group by 
    claim_id
    , data_source
    , diagnosis_column
    , poa.present_on_admit_code
