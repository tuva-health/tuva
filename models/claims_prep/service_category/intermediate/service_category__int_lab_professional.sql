with service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    medical_claim_sk
    , 'ancillary' as service_category_1
    , 'lab' as service_category_2
    , 'lab' as service_category_3
from service_category__stg_professional
where place_of_service_code = '81'
    or ccs_category in (
        '233' -- lab
        , '235' --other lab
        , '234' --pathology
    )
