{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select
    claim_id
    , data_source
    , min(normalized_claim_start_date) as minimum_claim_start_date
    , max(normalized_claim_end_date) as maximum_claim_end_date
    , min(normalized_admission_date) as minimum_admission_date
    , max(normalized_discharge_date) as maximum_discharge_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__int_medical_claim_date_normalize') }}
where claim_type = 'institutional'
group by
    claim_id
    , data_source

union all

select
    claim_id
    , data_source
    , min(normalized_claim_start_date) as minimum_claim_start_date
    , max(normalized_claim_end_date) as maximum_claim_end_date
    , null as minimum_admission_date
    , null as maximum_discharge_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__int_medical_claim_date_normalize') }}
where claim_type = 'professional'
group by
    claim_id
    , data_source