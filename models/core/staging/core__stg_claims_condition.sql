/*
1. Unpivot wide to tall
2. Derive rank based on column name
3. Derive recorded_date as earliest of admission_date, claim_start_date, discharge_date, claim_end_date
4. Map source diagnosis codes to the ICD9CM and ICD10CM terminology tables.

TODO:
*/
with normalized_input__medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'normalized_input__medical_claim') }}
)
, icd10codes as (
    select *
    from {{ ref('tuva_data_assets', 'icd_10_cm') }}
)
, unpivoted_conditions as (
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 1 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_1 as diagnosis_code
        , diagnosis_poa_1 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_1 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 2 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_2 as diagnosis_code
        , diagnosis_poa_2 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_2 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 3 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_3 as diagnosis_code
        , diagnosis_poa_3 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_3 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 4 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_4 as diagnosis_code
        , diagnosis_poa_4 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_4 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 5 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_5 as diagnosis_code
        , diagnosis_poa_5 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_5 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 6 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_6 as diagnosis_code
        , diagnosis_poa_6 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_6 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 7 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_7 as diagnosis_code
        , diagnosis_poa_7 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_7 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 8 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_8 as diagnosis_code
        , diagnosis_poa_8 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_8 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 9 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_9 as diagnosis_code
        , diagnosis_poa_9 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_9 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 10 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_10 as diagnosis_code
        , diagnosis_poa_10 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_10 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 11 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_11 as diagnosis_code
        , diagnosis_poa_11 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_11 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 12 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_12 as diagnosis_code
        , diagnosis_poa_12 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_12 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 13 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_13 as diagnosis_code
        , diagnosis_poa_13 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_13 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 14 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_14 as diagnosis_code
        , diagnosis_poa_14 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_14 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 15 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_15 as diagnosis_code
        , diagnosis_poa_15 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_15 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 16 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_16 as diagnosis_code
        , diagnosis_poa_16 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_16 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 17 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_17 as diagnosis_code
        , diagnosis_poa_17 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_17 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 18 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_18 as diagnosis_code
        , diagnosis_poa_18 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_18 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 19 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_19 as diagnosis_code
        , diagnosis_poa_19 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_19 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 20 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_20 as diagnosis_code
        , diagnosis_poa_20 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_20 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 21 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_21 as diagnosis_code
        , diagnosis_poa_21 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_21 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 22 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_22 as diagnosis_code
        , diagnosis_poa_22 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_22 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 23 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_23 as diagnosis_code
        , diagnosis_poa_23 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_23 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 24 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_24 as diagnosis_code
        , diagnosis_poa_24 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_24 is not null

    union all

    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 25 as diagnosis_rank
        , diagnosis_code_type
        , diagnosis_code_25 as diagnosis_code
        , diagnosis_poa_25 as diagnosis_poa
    from normalized_input__medical_claim
    where diagnosis_code_25 is not null
)
select
    {{ dbt_utils.generate_surrogate_key(['medical_claim_sk', 'diagnosis_rank']) }} as condition_sk
    , medical_claim_sk
    , data_source
    , claim_id
    , member_id
    , min(coalesce(admission_date, claim_start_date, discharge_date, claim_end_date)) over(partition by data_source, claim_id) as recorded_date
    , diagnosis_rank as rank_num
    , diagnosis_code_type as source_code_type
    , diagnosis_code as source_code
    , case when icd10codes.icd_10_cm is not null then 'icd-10-cm' end as normalized_code_type
    , icd10codes.icd_10_cm as normalized_code
    , diagnosis_poa as present_on_admit_code
from unpivoted_conditions
    left join icd10codes
        on unpivoted_conditions.diagnosis_code = icd10codes.icd_10_cm
        and unpivoted_conditions.diagnosis_code_type = 'icd-10-cm'