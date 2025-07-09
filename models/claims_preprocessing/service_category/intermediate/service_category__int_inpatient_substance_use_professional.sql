with service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    medical_claim_sk
    , 'inpatient' as service_category_1
    , 'inpatient substance use' as service_category_2
    , 'inpatient substance use' as service_category_3
from service_category__stg_professional
where place_of_service_code in ('55')
