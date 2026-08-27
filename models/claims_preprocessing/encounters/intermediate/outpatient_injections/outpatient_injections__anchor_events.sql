{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

with multiple_sources as (
select distinct
    med.patient_data_source_id
    , med.data_source
    , med.start_date
from {{ ref('encounters__stg_medical_claim') }} as med
inner join {{ ref('encounters__stg_outpatient_institutional') }} as outpatient
    on med.claim_id = outpatient.claim_id
    and med.data_source = outpatient.data_source
where substring(med.hcpcs_code, 1, 1) = 'J'
)


select distinct
    patient_data_source_id
    , data_source
    , start_date
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from multiple_sources
