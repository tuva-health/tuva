
-- Staging model for the input layer:
-- stg_diagnosis input layer model.
-- This contains one row for every unique diagnosis each patient has.


{{ config(enabled=var('readmissions_enabled',var('tuva_packages_enabled',True))) }}

with acute_institutional_claims as (
select distinct
    encounter_id
,   claim_id
from {{ ref('claims_preprocessing__medical_claim_enhanced') }}
where encounter_type = 'acute inpatient'
    and claim_type = 'institutional'
)

select distinct
    cast(a.encounter_id as {{ dbt.type_string() }}) as encounter_id
,   cast(a.code as {{ dbt.type_string() }}) as diagnosis_code
,   cast(a.diagnosis_rank as integer) as diagnosis_rank
from {{ var('condition') }} a
inner join  acute_institutional_claims b
    on a.claim_id = b.claim_id
where code_type = 'icd-10-cm'
