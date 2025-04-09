{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


with pivot_diagnosis as (
    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_1' as diagnosis_column
        , diagnosis_code_1 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_1 is not null


    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_2' as diagnosis_column
        , diagnosis_code_2 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_2 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_3' as diagnosis_column
        , diagnosis_code_3 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_3 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_4' as diagnosis_column
        , diagnosis_code_4 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_4 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_5' as diagnosis_column
        , diagnosis_code_5 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_5 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_6' as diagnosis_column
        , diagnosis_code_6 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_6 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_7' as diagnosis_column
        , diagnosis_code_7 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_7 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_8' as diagnosis_column
        , diagnosis_code_8 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_8 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_9' as diagnosis_column
        , diagnosis_code_9 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_9 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_10' as diagnosis_column
        , diagnosis_code_10 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_10 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_11' as diagnosis_column
        , diagnosis_code_11 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_11 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_12' as diagnosis_column
        , diagnosis_code_12 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_12 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_13' as diagnosis_column
        , diagnosis_code_13 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_13 is not null
    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_14' as diagnosis_column
        , diagnosis_code_14 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_14 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_15' as diagnosis_column
        , diagnosis_code_15 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_15 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_16' as diagnosis_column
        , diagnosis_code_16 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_16 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_17' as diagnosis_column
        , diagnosis_code_17 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_17 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_18' as diagnosis_column
        , diagnosis_code_18 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_18 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_19' as diagnosis_column
        , diagnosis_code_19 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_19 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_20' as diagnosis_column
        , diagnosis_code_20 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_20 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_21' as diagnosis_column
        , diagnosis_code_21 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_21 is not null
    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_22' as diagnosis_column
        , diagnosis_code_22 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_22 is not null

    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_23' as diagnosis_column
        , diagnosis_code_23 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_23 is not null
    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_24' as diagnosis_column
        , diagnosis_code_24 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_24 is not null
    union all

    select
        claim_id
        , claim_type
        , data_source
        , diagnosis_code_type
        , 'diagnosis_code_25' as diagnosis_column
        , diagnosis_code_25 as diagnosis_code
    from {{ ref('normalized_input__stg_medical_claim') }}
	where diagnosis_code_25 is not null)

select
    claim_id
    , data_source
    , diagnosis_code_type
    , diagnosis_column
    , coalesce(icd_9.icd_9_cm, icd_10.icd_10_cm) as normalized_diagnosis_code
    , count(*) as diagnosis_code_occurrence_count
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from pivot_diagnosis as piv
left outer join {{ ref('terminology__icd_10_cm') }} as icd_10
    on replace(piv.diagnosis_code, '.', '') = icd_10.icd_10_cm
    and piv.diagnosis_code_type = 'icd-10-cm'
left outer join {{ ref('terminology__icd_9_cm') }} as icd_9
    on replace(piv.diagnosis_code, '.', '') = icd_9.icd_9_cm
    and piv.diagnosis_code_type = 'icd-9-cm'
where claim_type <> 'undetermined'
group by
    claim_id
    , data_source
    , diagnosis_code_type
    , diagnosis_column
    , coalesce(icd_9.icd_9_cm, icd_10.icd_10_cm)
