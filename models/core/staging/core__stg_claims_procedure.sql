with normalized_input__medical_claim as (
    select *
    from {{ ref('normalized_input__medical_claim') }}
) 
, icd_10_pcs as (
    select *
    from {{ ref('tuva_data_assets', 'icd_10_pcs') }}
)
, unpivoted_procedures as (
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 1 as procedure_rank
        , procedure_code_type
        , procedure_code_1 as procedure_code
        , procedure_date_1 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_1 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 2 as procedure_rank
        , procedure_code_type
        , procedure_code_2 as procedure_code
        , procedure_date_2 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_2 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 3 as procedure_rank
        , procedure_code_type
        , procedure_code_3 as procedure_code
        , procedure_date_3 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_3 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 4 as procedure_rank
        , procedure_code_type
        , procedure_code_4 as procedure_code
        , procedure_date_4 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_4 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 5 as procedure_rank
        , procedure_code_type
        , procedure_code_5 as procedure_code
        , procedure_date_5 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_5 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 6 as procedure_rank
        , procedure_code_type
        , procedure_code_6 as procedure_code
        , procedure_date_6 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_6 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 7 as procedure_rank
        , procedure_code_type
        , procedure_code_7 as procedure_code
        , procedure_date_7 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_7 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 8 as procedure_rank
        , procedure_code_type
        , procedure_code_8 as procedure_code
        , procedure_date_8 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_8 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 9 as procedure_rank
        , procedure_code_type
        , procedure_code_9 as procedure_code
        , procedure_date_9 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_9 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 10 as procedure_rank
        , procedure_code_type
        , procedure_code_10 as procedure_code
        , procedure_date_10 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_10 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 11 as procedure_rank
        , procedure_code_type
        , procedure_code_11 as procedure_code
        , procedure_date_11 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_11 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 12 as procedure_rank
        , procedure_code_type
        , procedure_code_12 as procedure_code
        , procedure_date_12 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_12 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 13 as procedure_rank
        , procedure_code_type
        , procedure_code_13 as procedure_code
        , procedure_date_13 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_13 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 14 as procedure_rank
        , procedure_code_type
        , procedure_code_14 as procedure_code
        , procedure_date_14 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_14 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 15 as procedure_rank
        , procedure_code_type
        , procedure_code_15 as procedure_code
        , procedure_date_15 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_15 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 16 as procedure_rank
        , procedure_code_type
        , procedure_code_16 as procedure_code
        , procedure_date_16 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_16 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 17 as procedure_rank
        , procedure_code_type
        , procedure_code_17 as procedure_code
        , procedure_date_17 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_17 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 18 as procedure_rank
        , procedure_code_type
        , procedure_code_18 as procedure_code
        , procedure_date_18 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_18 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 19 as procedure_rank
        , procedure_code_type
        , procedure_code_19 as procedure_code
        , procedure_date_19 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_19 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 20 as procedure_rank
        , procedure_code_type
        , procedure_code_20 as procedure_code
        , procedure_date_20 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_20 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 21 as procedure_rank
        , procedure_code_type
        , procedure_code_21 as procedure_code
        , procedure_date_21 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_21 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 22 as procedure_rank
        , procedure_code_type
        , procedure_code_22 as procedure_code
        , procedure_date_22 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_22 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 23 as procedure_rank
        , procedure_code_type
        , procedure_code_23 as procedure_code
        , procedure_date_23 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_23 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 24 as procedure_rank
        , procedure_code_type
        , procedure_code_24 as procedure_code
        , procedure_date_24 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_24 is not null
    union
    select
        medical_claim_sk
        , data_source
        , claim_id
        , member_id
        , admission_date
        , claim_start_date
        , discharge_date
        , claim_end_date
        , 25 as procedure_rank
        , procedure_code_type
        , procedure_code_25 as procedure_code
        , procedure_date_25 as procedure_date
        , rendering_npi
    from normalized_input__medical_claim
    where procedure_code_25 is not null

)
select
    {{ dbt_utils.generate_surrogate_key(['medical_claim_sk', 'procedure_rank']) }} as procedure_sk
    , medical_claim_sk
    , data_source
    , claim_id
    , member_id
    , rendering_npi
    , coalesce(procedure_date, admission_date, claim_start_date, discharge_date, claim_end_date) as procedure_date
    , procedure_rank as rank_num
    , procedure_code_type as source_code_type
    , procedure_code as source_code
    , case when icd.icd_10_pcs is not null then 'icd-10-pcs' end as normalized_code_type
    , icd.icd_10_pcs as normalized_code
from unpivoted_procedures
    left outer join icd_10_pcs as icd
    on unpivoted_procedures.procedure_code = icd.icd_10_pcs