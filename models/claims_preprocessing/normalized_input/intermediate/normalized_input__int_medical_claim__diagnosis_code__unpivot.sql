with unpivot_cte as (
    select 
        surrogate_key
        , diagnosis_code_type
        , column_name
        , diagnosis_code
    from test_tuva_claims_preprocessing.normalized_input__stg_medical_claim
    unpivot (
        diagnosis_code for column_name in (
            diagnosis_code_1, diagnosis_code_2, diagnosis_code_3, diagnosis_code_4, diagnosis_code_5,
            diagnosis_code_6, diagnosis_code_7, diagnosis_code_8, diagnosis_code_9, diagnosis_code_10,
            diagnosis_code_11, diagnosis_code_12, diagnosis_code_13, diagnosis_code_14, diagnosis_code_15,
            diagnosis_code_16, diagnosis_code_17, diagnosis_code_18, diagnosis_code_19, diagnosis_code_20,
            diagnosis_code_21, diagnosis_code_22, diagnosis_code_23, diagnosis_code_24, diagnosis_code_25
        )
    ) as unpivoted
    where diagnosis_code is not null
)
select 
    surrogate_key
    , column_name
    , diagnosis_code_type as source_code_type
    , diagnosis_code as source_code
    , case when b.icd_10_cm is not null then 'icd-10-cm' end as normalized_code_type
    , b.icd_10_cm as normalized_code
from unpivot_cte as a
left join test_tuva_terminology.icd_10_cm as b
on a.diagnosis_code = b.icd_10_cm
and a.diagnosis_code_type = 'icd-10-cm'