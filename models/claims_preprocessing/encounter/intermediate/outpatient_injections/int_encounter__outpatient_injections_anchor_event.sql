{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

with multiple_sources as (
select distinct
    med.patient_data_source_id
    , med.start_date
from {{ ref('int_encounter__claim_line') }} as med
inner join {{ ref('int_encounter__outpatient_institutional_service_type') }} as outpatient
    on med.claim_id = outpatient.claim_id
where substring(med.hcpcs_code, 1, 1) = 'J'
)


select distinct
    patient_data_source_id
    , start_date
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from multiple_sources
