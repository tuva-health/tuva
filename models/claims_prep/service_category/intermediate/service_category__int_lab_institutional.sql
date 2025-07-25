with service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
)
select
    medical_claim_sk
    , 'ancillary' as service_category_1
    , 'lab' as service_category_2
    , 'lab' as service_category_3
from service_category__stg_outpatient_institutional
where bill_type_code in ('14')
    or ccs_category in (
        '233' -- lab
        , '235' --other lab
        , '234' --pathology
    )
