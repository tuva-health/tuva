with service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'outpatient hospital or clinic' as service_category_2
    , 'outpatient hospital or clinic' as service_category_3
from service_category__stg_professional
where
    place_of_service_code in ('15', '17', '19', '22', '49', '50', '60', '71', '72')
