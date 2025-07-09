with service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
)
select
    medical_claim_sk
    , 'ancillary' as service_category_1
    , 'durable medical equipment' as service_category_2
    , 'durable medical equipment' as service_category_3
from service_category__stg_outpatient_institutional
where ccs_category = '243'
