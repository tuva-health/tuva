{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select
    claim_id
    , data_source
    , diagnosis_column
    , count(*) as distinct_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('header_validation__int_diagnosis_normalize') }}
where normalized_diagnosis_code is not null
and diagnosis_column like '%DIAGNOSIS%'
group by
    claim_id
    , data_source
    , diagnosis_column



