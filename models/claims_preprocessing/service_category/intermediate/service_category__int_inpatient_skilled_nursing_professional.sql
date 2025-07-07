with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__dme_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__int_dme_professional') }}
)
select distinct
    med.medical_claim_sk
    , 'inpatient' as service_category_1
    , 'skilled nursing' as service_category_2
    , 'skilled nursing' as service_category_3
from service_category__stg_medical_claim as med
    left outer join service_category__dme_professional as b
    on med.medical_claim_sk = b.medical_claim_sk
where med.claim_type = 'professional'
    and med.place_of_service_code in ('31', '32')
    and b.medical_claim_sk is null
