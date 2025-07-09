/*
 * Determines whether a claims service type is "professional". It must be a professional claim.
 */
with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
)
select *
from service_category__stg_medical_claim
where claim_type = 'professional'
