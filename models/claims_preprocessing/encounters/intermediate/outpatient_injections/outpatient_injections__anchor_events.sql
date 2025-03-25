{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with multiple_sources as (
select distinct
    med.patient_data_source_id
    , med.start_date
from {{ ref('encounters__stg_medical_claim') }} as med
inner join {{ ref('encounters__stg_outpatient_institutional') }} as outpatient
    on med.claim_id = outpatient.claim_id
where substring(med.hcpcs_code, 1, 1) = 'J'
)


select distinct
    patient_data_source_id
    , start_date
, '{{ var('tuva_last_run') }}' as tuva_last_run
from multiple_sources
