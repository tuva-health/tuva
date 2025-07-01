with unpivot_cte as (
    select 
        surrogate_key
        , column_name
        , diagnosis_poa
    from test_tuva_claims_preprocessing.normalized_input__stg_medical_claim
    unpivot (
        diagnosis_poa for column_name in (
            diagnosis_poa_1, diagnosis_poa_2, diagnosis_poa_3, diagnosis_poa_4, diagnosis_poa_5,
            diagnosis_poa_6, diagnosis_poa_7, diagnosis_poa_8, diagnosis_poa_9, diagnosis_poa_10,
            diagnosis_poa_11, diagnosis_poa_12, diagnosis_poa_13, diagnosis_poa_14, diagnosis_poa_15,
            diagnosis_poa_16, diagnosis_poa_17, diagnosis_poa_18, diagnosis_poa_19, diagnosis_poa_20,
            diagnosis_poa_21, diagnosis_poa_22, diagnosis_poa_23, diagnosis_poa_24, diagnosis_poa_25
        )
    ) as unpivoted
    where diagnosis_poa is not null
)
select
    surrogate_key
    , column_name
    , diagnosis_poa as source_poa
    , case when b.icd_10_cm is not null then 'icd-10-cm' end as normalized_poa_type
    , b.icd_10_cm as normalized_poa
from unpivot_cte as a
left join test_tuva_terminology.icd_10_cm as b
on a.diagnosis_poa = b.icd_10_cm