/*
 * Determines whether a claims service type is "outpatient". It must be an institutional claim, and not be
 * considered inpatient.
 */
with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_inpatient_institutional') }}
)
select med.*
from service_category__stg_medical_claim as med
    left outer join service_category__stg_inpatient_institutional as i
    on med.medical_claim_sk = i.medical_claim_sk
where i.medical_claim_sk is null
    and med.claim_type = 'institutional'
