with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
)
select
    medical_claim_sk
    , 'ancillary' as service_category_1
    , 'ambulance' as service_category_2
    , 'ambulance' as service_category_3
from service_category__stg_medical_claim
where
  claim_type = 'professional'
  and (
    hcpcs_code between 'A0425' and 'A0436'
    or place_of_service_code in ('41', '42')
  )
