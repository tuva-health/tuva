/*
 * Determines whether a claims service type is "office based". It must be a professional claim.
 */
with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
)
select
    medical_claim_sk
    , 'office based' as service_type
from service_category__stg_medical_claim
where claim_type = 'professional'
    and place_of_service_code in (
        '02' --telehealth
        , '10' --telehealth
        , '11' -- office
        )
