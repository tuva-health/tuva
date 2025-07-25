with service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'outpatient substance use' as service_category_2
    , 'outpatient substance use' as service_category_3
from service_category__stg_professional
where (
    default_ccsr_category_description_op in (
        'MBD026'
        , 'SYM008'
        , 'MBD025'
        , 'SYM009'
        , 'MBD034'
    )
    and place_of_service_code <> '11')
    or place_of_service_code in ('57', '58')
