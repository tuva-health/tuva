/*
 * Determines whether a claims service type is "outpatient". It must be an institutional claim, and not be
 * considered inpatient.
 */
with encounters__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'encounters__stg_medical_claim') }}
),
encounters__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'encounters__stg_inpatient_institutional') }}
)
select med.*
from encounters__stg_medical_claim as med
    left outer join encounters__stg_inpatient_institutional as i
    on med.medical_claim_sk = i.medical_claim_sk
where i.medical_claim_sk is null
    and med.claim_type = 'institutional'
