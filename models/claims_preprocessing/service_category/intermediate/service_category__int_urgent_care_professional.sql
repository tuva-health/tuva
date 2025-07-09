with service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'urgent care' as service_category_2
    , 'urgent care' as service_category_3
from service_category__stg_professional
where
    (place_of_service_code in ('20')
    or hcpcs_code in ('S9088', '99051', 'S9083'))
