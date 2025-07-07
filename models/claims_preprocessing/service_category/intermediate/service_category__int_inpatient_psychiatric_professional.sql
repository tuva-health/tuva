with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
)
select
    medical_claim_sk
    , 'inpatient' as service_category_1
    , 'inpatient psychiatric' as service_category_2
    , 'inpatient psychiatric' as service_category_3
from service_category__stg_medical_claim
where claim_type = 'professional'
  and place_of_service_code in ('51', '56')
